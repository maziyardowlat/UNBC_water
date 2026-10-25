# Weather station source

`02FW005_tidy_CR350_1379_20250521.csv` is an unchanged copy of the user-supplied CSV.

- Station: 02FW005; logger: CR350-1379.
- 65,457 observations, 2023-07-04 14:00:00 through 2025-05-16 10:00:00.
- All source timestamps specify UTC offset -07:00; the viewer preserves that fixed offset.
- The second CSV line supplies units, not observations. Unknown units are not inferred.
- Each variable's `_Flag` column is preserved, including multiple codes per observation.
- Code display names follow `FLAG_DISPLAY_NAMES` in the local `weatherqa_qc/app.py` application.
- The viewer does not rerun QC or interpret all non-P codes as failures.
- Selection downloads preserve all source columns, their flags and the units row for the selected records. The original download preserves the entire input file.

Open the application with `?view=weather` to select the weather tab directly. This addition is local; no deployment was performed.
