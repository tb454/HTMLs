import csv

INPUT = "vendor_material_map.csv"
OUTPUT = "vendor_material_map_clean.csv"

with open(INPUT, "r", encoding="utf-8-sig", newline="") as f_in, \
     open(OUTPUT, "w", encoding="utf-8", newline="") as f_out:

    rdr = csv.reader(f_in)
    w = csv.writer(f_out)

    header = next(rdr, None)
    if header is None:
        raise SystemExit("Empty input file")
    # Force correct header just in case
    w.writerow(["vendor", "material_vendor", "material_canonical"])

    for row in rdr:
        if not row:
            continue

        # Rows that start with C&Y Global split wrong because of the comma
        if row[0].startswith("C&Y Global"):
            # Example raw row:
            # ['C&Y Global', ' Inc. / Pro Metal Recycling', '#3 copper', 'Sheet Copper']
            if len(row) < 4:
                continue
            vendor = "C&Y Global, Inc. / Pro Metal Recycling"
            material_vendor = row[-2].strip()     # second to last field
            material_canonical = row[-1].strip()  # last field
        else:
            if len(row) < 3:
                continue
            vendor = row[0].strip()
            material_vendor = row[1].strip()
            material_canonical = row[2].strip()

        if not (vendor and material_vendor and material_canonical):
            continue

        w.writerow([vendor, material_vendor, material_canonical])

print("Wrote cleaned CSV to", OUTPUT)
