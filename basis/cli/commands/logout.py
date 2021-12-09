from basis.cli.services import auth


def logout():
    """Log out of your Basis account"""
    auth.logout()
