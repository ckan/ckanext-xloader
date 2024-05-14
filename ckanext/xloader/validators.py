from ckan.plugins.toolkit import asbool


def datastore_fields_validator(value, context):
    if 'strip_extra_white' not in value:
        # default to True
        value['strip_extra_white'] = True

    # bool value for strip_extra_white
    value['strip_extra_white'] = asbool(value['strip_extra_white'])

    return value
