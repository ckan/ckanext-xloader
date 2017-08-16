import ckanext.datastore.logic.auth as auth


def shift_submit(context, data_dict):
    return auth.datastore_auth(context, data_dict)


def shift_status(context, data_dict):
    return auth.datastore_auth(context, data_dict)
