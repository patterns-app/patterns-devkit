import inspect
import os
from shutil import copyfile

from snapflow.project import default_project_tmpl

SNAPFLOW_PROJECT_PACKAGE_NAME = "_snapflow"
SNAPFLOW_PROJECT_FILE_NAME = f"{SNAPFLOW_PROJECT_PACKAGE_NAME}.py"


def init_project_in_dir(dir: str):
    pth = os.path.join(dir, SNAPFLOW_PROJECT_FILE_NAME)
    if os.path.exists(pth):
        raise FileExistsError("Project file already exists")
    copyfile(inspect.getabsfile(default_project_tmpl), pth)
    # OR:
    # project_str = inspect.getsource(default_project_tmpl)
    # with open(SNAPFLOW_PROJECT_FILE_NAME, "w") as f:
    #     f.write(project_str)
