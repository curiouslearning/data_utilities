import facebook as fb
import importers.delete_and_load_facebook as dlfb


def import_data(event, context="local"):
    dlfb.get_facebook_data()
    return "ok"
