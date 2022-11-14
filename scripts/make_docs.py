import re

s = open("node.md").read()
s += open("meth.md").read()

s = re.sub(r"@classmethod\s*", "", s)
s = re.sub(r"cls,?\s*", "", s)
s = re.sub(r"####", "###", s)
s = re.sub(r"::\s*", "\n\n```python #FIXME\n", s)

with open("docs.md", "w") as f:
    f.write(s)
