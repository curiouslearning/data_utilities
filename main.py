import importers.facebook as fb
import settings


def import_data(event):
    fb.get_facebook_data()
    return "ok"
