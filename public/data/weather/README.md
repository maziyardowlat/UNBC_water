# Weather station data

Only observation files from each station's `03_Data_Tidy` folder in the supplied
`OneDrive_1_11-7-2026` export are included. Raw logger exports, archived Excel
workbooks and earlier repository copies are not used as visualization data.

The station catalog contains eight named sites. Seven have tidy CSVs, totaling
930,826 observations. François Lake has no supplied tidy CSV and appears on the
map as awaiting tidy data.

| Station | Name | Tidy observations |
| --- | --- | ---: |
| 02FW003 | Nadina Spawning Channel | 40,239 |
| 02FW005 | Cheslatta Lake | 65,457 |
| 02FW006 | Nulki Lake | 39,251 |
| 02FW007 | François Lake | None supplied |
| 02FW008 | Isle Pierre | 12,079 |
| 02MF005 | Browntop Mountain | 666,797 |
| 02MF009 | Quesnel Lake West Arm | 43,173 |
| 02PE002 | Kiskatinaw | 63,830 |

Browntop and Kiskatinaw's supplied tidy files are named `*_tidy_historical.csv`.
They are taken from `03_Data_Tidy`, not the raw or historical archive directories.

Original CSV copies are byte-for-byte unchanged. Monthly `.csv.gz` views retain
every source column, value, units row and QC code. Monthly views are lossless:
concatenating their observation rows in month order reproduces the source rows.
The browser fetches only the months needed for the selected date range. Browntop's
long record is displayed in windows of up to one year to bound browser memory;
its original download contains the entire record. All supplied UTC offsets are
-07:00 and the viewer preserves that fixed offset.

The map uses names from the station folders and coordinates from the supplied
field reports or archived NHG wiki pages. Each catalog entry records its location
source. François Lake uses its supplied 2025 field report, not the differing
coordinates in an older local station configuration. `MetadataLog.xlsx` copies
are supplied for download alongside each station.

QC is not rerun. Source QC flags, identifiers (including any source typos) and
units are preserved. Unknown units are not inferred, and non-P contextual codes
are not automatically treated as QC failures. Display names for QC codes follow
the weatherqa_qc application.

To rebuild the catalog and monthly views from a supplied folder:

```sh
python scripts/import_weather.py /path/to/OneDrive_export
```

The original Browntop tidy CSV is approximately 209 MB. Its download is a ZIP
containing the unchanged CSV; extract the ZIP to open the full record. The
importer packages original CSVs above 50 MiB this way and rejects ZIPs that
still exceed GitHub's 100 MiB file limit. The catalog SHA-256 is for the CSV
inside the ZIP. The existing deploy script excludes `data/*`.

Open the application with `?view=weather` to select the weather tab directly.
