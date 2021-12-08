from basis.cli.newapp import app
from basis.cli.services import auth


@app.command()
def logout():
    """Log out of your Basis account"""
    auth.logout()
