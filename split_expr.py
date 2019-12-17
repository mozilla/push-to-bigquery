from mo_files import File
from mo_logs import strings, Log

base = "jx_sqlite/expressions"

def main():
    lines = File(base+".py").read_lines()


    eoi = max(*(i for i, line in enumerate(lines) if line.startswith("import") or line.startswith("from ")))
    imports = lines[:eoi]
    residue = []
    curr = eoi + 1
    while curr < len(lines):
        curr_line = lines[curr]
        if curr_line.startswith("class "):
            filename = class_to_file(strings.between(" class", "("))
            end = curr + 1
            while lines[end].startswith("    "):
                end += 1
            (File(base) / (filename + ".py")).write_lines(imports + [""] + lines[curr:end])
            curr = end - 1
        curr += 1
        residue.append(curr_line)
    (File(base) / "utils.py").write_lines(imports + residue)


def class_to_file(classname):
    output = []
    for c in classname:
        if 'A'<=c<='Z':
            output.append("_")
            output.append(c.lower())
        else:
            output.append(c)
    return "".join(output)


if __name__=="__main__":
    main()