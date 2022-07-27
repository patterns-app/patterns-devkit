from patterns.cli.services import logout as logout_service
from patterns.cli.services.api import reset_session_auth
from patterns.cli.services.output import sprint


def logout():
    """Log out of your Patterns account"""
    reset_session_auth()
    logout_service.logout()
    sprint("[success]Logged out")
