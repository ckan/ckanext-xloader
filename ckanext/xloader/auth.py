import ckanext.datastore.logic.auth as auth


def xloader_submit(context, data_dict):
    return auth.datastore_auth(context, data_dict)


def xloader_status(context, data_dict):
    return auth.datastore_auth(context, data_dict)
