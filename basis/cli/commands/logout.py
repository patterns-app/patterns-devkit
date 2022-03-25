from basis.cli.services import logout as logout_service


def logout():
    """Log out of your Basis account"""
    logout_service.logout()
