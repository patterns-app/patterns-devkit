from pathlib import Path

import typer.testing

from basis.cli.config import read_local_basis_config
from basis.cli.main import app
from basis.cli.services.api import API_BASE_URL, Endpoints
from tests.cli.base import request_mocker, set_tmp_dir

runner = typer.testing.CliRunner()


def test_generate_graph(tmp_path: Path):
    dr = set_tmp_dir(tmp_path).parent
    name = "testgraph"
    runner.invoke(app, ['create', 'graph'], f'{dr / name}\n')
    assert name in (Path(dr) / name / "graph.yml").read_text()



# TODO
# def test_generate_graph_with_name(tmp_path: Path):
#     dr = set_tmp_dir(tmp_path).parent
#     name = "testgraph"
#     path = dr / "pth" / "projname"
#     runner.invoke(app, ['create', 'graph', f'--name={name}', f"'{path}'"])
#     assert name in (Path(dr) / path / "graph.yml").read_text()

# def test_generate_node():
#     dr = set_tmp_dir()
#     command_tester = get_test_command("generate")
#     name = f"test_{random.randint(0, 10000)}.py".lower()
#     pth = "proj/app1/" + name
#     inputs = "\n".join(["\n", "python"]) + "\n"
#     command_tester.execute(f"node {pth}", inputs=inputs)
#     assert os.path.exists(Path(dr) / pth)
