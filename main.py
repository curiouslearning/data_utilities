import importers.facebook as fb
import settings


def import_data(event, context="local"):
    fb.get_facebook_data()
    return "ok"
