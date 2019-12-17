from mo_files import File
from mo_logs import strings, Log

base = "vendor/jx_sqlite/expressions"
import_prefix = "from jx_sqlite.expressions."

def main():
    lines = list(File(base + ".py").read_lines())

    eoi = max(
        *(
            i
            for i, line in enumerate(lines)
            if line.startswith("import") or line.startswith("from ")
        )
    )
    imports = lines[:eoi]
    imports.append(import_prefix+"_utils import json_type_to_sql_type, SQLang, check")

    # FIND ALL NEW IMPORTS
    curr = eoi + 1
    while curr < len(lines):
        curr_line = lines[curr]
        if curr_line.startswith("class "):
            classname = strings.between(curr_line, "class ", "(")
            filename = class_to_file(classname)
            imports.append(import_prefix+filename+" import "+classname)
        curr += 1

    # MAKE ALL NEW FILES
    residue = []
    curr = eoi + 1
    while curr < len(lines):
        curr_line = lines[curr]
        if curr_line.startswith("class "):
            filename = class_to_file(strings.between(curr_line, "class ", "("))
            end = curr + 1
            while not lines[end].strip() or lines[end].startswith("    "):
                end += 1
            file = File(base) / (filename + ".py")
            file.delete()
            file.extend(
                imports + [""] + lines[curr:end]
            )
            curr = end
        else:
            residue.append(curr_line)
            curr += 1
    file = File(base) / "_utils.py"
    file.delete()
    file.extend(imports + residue)


def class_to_file(classname):
    output = []
    for i, c in enumerate(classname):
        if "A" <= c <= "Z":
            if i and (("a" <= classname[i - 1] <= "z") or ("a" <= classname[i + 1] <= "z")):
                output.append("_")
            output.append(c.lower())
        else:
            output.append(c)
    filename = "".join(output)
    # if filename.endswith("_op"):
    #     filename = filename[:-3]
    return filename


if __name__ == "__main__":
    main()
