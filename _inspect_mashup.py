import base64
import html
import re
import zipfile


def main() -> None:
    with zipfile.ZipFile("Primer_zaprosov.xlsx") as zf:
        raw = zf.read("customXml/item1.xml")

    text = raw.decode("utf-16", errors="ignore")
    m = re.search(r"<DataMashup[^>]*>(.*)</DataMashup>", text, re.S)
    if not m:
        print("DataMashup payload not found")
        return

    payload = re.sub(r"\s+", "", m.group(1))
    decoded = base64.b64decode(payload + "===", validate=False).decode("utf-8", errors="ignore")

    for query_path in ("Section1/ZNOM", "Section1/REESTR"):
        i = decoded.find(query_path)
        print(f"\n=== {query_path} at {i} ===")
        if i == -1:
            continue

        chunk = decoded[i : i + 25000]
        entry_pattern = re.compile(r'Entry Type="([^"]+)" Value="([^"]*)"')
        for typ, val in entry_pattern.findall(chunk):
            val_unescaped = html.unescape(val)
            typ_l = typ.lower()
            if (
                "formula" in typ_l
                or "query" in typ_l
                or "document" in typ_l
                or "expression" in typ_l
                or typ in {"Name", "NavigationStepName", "FillTarget", "ResultType", "LoadEnabled", "FillEnabled"}
            ):
                print(f"{typ} => {val_unescaped[:300]!r}")

        # Also print all potential base64 values that could contain M formulas
        for typ, val in entry_pattern.findall(chunk):
            if len(val) > 200 and re.fullmatch(r"[A-Za-z0-9+/=]+", val):
                try:
                    s = base64.b64decode(val + "===", validate=False).decode("utf-8", errors="ignore")
                except Exception:
                    continue
                if any(k in s for k in ["let", "in", "Table.", "Folder.", "Excel.Workbook", "File.Contents"]):
                    print(f"\nBASE64 FIELD {typ}:")
                    print(s[:3000])


if __name__ == "__main__":
    main()
