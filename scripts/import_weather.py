"""Import supplied tidy CSVs without rerunning QC; create compressed monthly views.

Usage: python scripts/import_weather.py /path/to/OneDrive_export
"""
import argparse
import csv
import gzip
import hashlib
import io
import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory
from zipfile import ZIP_DEFLATED, ZipFile


# Names follow the supplied station folders. Locations come from the supplied
# field reports (latest visit) or archived NHG wiki for the historical sites.
SITES = {
    '02FW003': ('Nadina Spawning Channel', 53.902461, -126.966122, '02FW003_FieldReport_20250820.pdf'),
    '02FW005': ('Cheslatta Lake', 53.721712, -125.641697, '02FW005_FieldReport_20250521.pdf'),
    '02FW006': ('Nulki Lake', 53.913409, -124.151613, '02FW006_FieldReport_20250711.pdf'),
    '02FW007': ('François Lake', 54.051470, -125.757264, '02FW007_FieldReport_20250521.pdf'),
    '02FW008': ('Isle Pierre', 53.957133, -123.258189, '02FW008_FieldReport_20251022.pdf'),
    '02MF005': ('Browntop Mountain', 52 + 42/60 + 28/3600, -(121 + 20/60 + 2.4/3600), 'Expsite_Browntop - NHG wiki.html'),
    '02MF009': ('Quesnel Lake West Arm', 52.489456, -121.429141, '02MF009_FieldReport_20250828.pdf'),
    '02PE002': ('Kiskatinaw', 55 + 45/60 + 4.2/3600, -(120 + 31/60 + 26.8/3600), 'Expsite_Kiskatinaw - NHG wiki.html'),
}


def original_download(source, output):
    with source.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    filename = source.name
    download = dict(name=filename, sha256=digest)
    if source.stat().st_size > 50 * 1024**2:
        filename = source.with_suffix('.zip').name
        with TemporaryDirectory(dir=output) as temp:
            archive = Path(temp) / filename
            with ZipFile(archive, 'w', compression=ZIP_DEFLATED, compresslevel=6) as bundle:
                with source.open('rb') as stream, bundle.open(source.name, 'w', force_zip64=True) as entry:
                    shutil.copyfileobj(stream, entry)
            if archive.stat().st_size > 100 * 1024**2:
                raise ValueError(f'{source}: even the ZIP exceeds 100 MiB; host it outside Git')
            archive.replace(output / filename)
        download['format'] = 'zip'
    else:
        shutil.copyfile(source, output / filename)
    download['url'] = f'/data/weather/{filename}'
    return download


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    args = parser.parse_args()
    output = Path(__file__).resolve().parents[1] / 'public/data/weather'
    output.mkdir(parents=True, exist_ok=True)
    stations = []
    for folder in sorted(args.source.iterdir()):
        if folder.name[:7] not in SITES:
            continue
        code = folder.name[:7]
        name, lat, lon, coord_source = SITES[code]
        station = dict(id=code, name=name, latitude=lat, longitude=lon,
                       coordinateSource=coord_source, sourceFolder=folder.name,
                       files=[], months=[], records=0, offsets=[])
        metadata = folder / '01_Metadata/MetadataLog.xlsx'
        if metadata.exists():
            dest = output / code / 'MetadataLog.xlsx'
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(metadata, dest)
            station['metadataUrl'] = f'/data/weather/{code}/MetadataLog.xlsx'
        csvs = sorted(folder.glob('03_Data_Tidy/*.csv'))
        if not csvs:
            station['status'] = 'pending'
            stations.append(station)
            print(code, name, '— no tidy CSV supplied', flush=True)
            continue
        station['status'] = 'available'
        # Each current station has one supplied tidy file. Reject ambiguity
        # rather than silently concatenating overlapping logger exports.
        if len(csvs) != 1:
            raise ValueError(f'{code}: expected one tidy file, found {len(csvs)}')
        source = csvs[0]
        station['files'].append(original_download(source, output))
        chunks = {}
        offsets = set()
        handles = []
        first = last = None
        with source.open(newline='', encoding='utf-8-sig') as stream:
            reader = csv.DictReader(stream)
            fields = reader.fieldnames
            units = next(reader)
            if 'TIMESTAMP' not in fields or 'UTC Offset' not in fields:
                raise ValueError(f'{source}: missing timestamp columns')
            station['units'] = units
            station['fields'] = fields
            try:
                for row in reader:
                    if None in row or any(v is None for v in row.values()):
                        raise ValueError(f'{source}: malformed CSV row {reader.line_num}')
                    if row.get('Station_ID') != code:
                        raise ValueError(f'{source}: unexpected station ID')
                    stamp = row['TIMESTAMP']
                    month = stamp[:7]
                    if month not in chunks:
                        path = output / code / f'{month}.csv.gz'
                        path.parent.mkdir(parents=True, exist_ok=True)
                        handle = io.TextIOWrapper(gzip.GzipFile(filename=path, mode='wb', mtime=0), encoding='utf-8', newline='')
                        handles.append(handle)
                        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator='\n')
                        writer.writeheader()
                        writer.writerow(units)
                        chunks[month] = (writer, dict(month=month, url=f'/data/weather/{code}/{month}.csv.gz', records=0))
                    writer, chunk = chunks[month]
                    writer.writerow(row)
                    chunk['records'] += 1
                    station['records'] += 1
                    offsets.add(row['UTC Offset'])
                    first = min(first, stamp) if first else stamp
                    last = max(last, stamp) if last else stamp
            finally:
                for handle in handles:
                    handle.close()
        station.update(start=first[:10], end=last[:10], offsets=sorted(offsets),
                       months=[chunks[m][1] for m in sorted(chunks)])
        stations.append(station)
        print(code, name, station['records'], 'records,', len(chunks), 'months', flush=True)
    (output / 'stations.json').write_text(json.dumps(stations, indent=2, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    main()
