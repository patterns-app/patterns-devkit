from typer import Argument

app_argument_help = (
    "The slug or uid of an app or app version, or the path to an app's graph.yml"
)
app_argument = Argument(None, help=app_argument_help, show_default=False)
