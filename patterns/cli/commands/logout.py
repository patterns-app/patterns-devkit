from patterns.cli.services import logout as logout_service


def logout():
    """Log out of your Patterns account"""
    logout_service.logout()
