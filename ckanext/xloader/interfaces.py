from ckan.plugins.interfaces import Interface


class IXloader(Interface):
    """
    The IXloader interface allows plugin authors to receive notifications
    before and after a resource is submitted to the xloader service, as
    well as determining whether a resource should be submitted in can_upload

    The before_submit function, when implemented
    """

    def can_upload(self, resource_id):
        """ This call when implemented can be used to stop the processing of
        the xloader submit function. This method will not be called if
        the resource format does not match those defined in the
        ckanext.xloader.formats config option or the default formats.

        If this function returns False then processing will be aborted,
        whilst returning True will submit the resource to the xloader
        service

        Note that before reaching this hook there is a prior check on the
        resource format, which depends on the value of
        the :ref:`ckanext.xloader.formats` configuration option (and requires
        the resource to have a format defined).

        :param resource_id: The ID of the resource that is to be
            pushed to the xloader service.

        Returns ``True`` if the job should be submitted and ``False`` if
        the job should be aborted

        :rtype: bool
        """
        return True

    def after_upload(self, context, resource_dict, dataset_dict):
        """ After a resource has been successfully upload to the datastore
        this method will be called with the resource dictionary and the
        package dictionary for this resource.

        :param context: The context within which the upload happened
        :param resource_dict: The dict represenstaion of the resource that was
            successfully uploaded to the datastore
        :param dataset_dict: The dict represenstation of the dataset containing
            the resource that was uploaded
        """
        pass

    def datastore_before_update(self, resource_id, existing_info, new_headers):
        """ Called by the loader just before it is about to modify the
        DataStore table for a resource (truncate, drop+recreate, or create).
        It allows plugins to inspect the difference between the current
        DataStore columns and the ones detected in the incoming file, for
        example to log an activity when columns are added, removed or
        renamed.

        :param resource_id: the ID of the resource whose DataStore table is
            about to be updated.
        :type resource_id: string

        :param existing_info: a mapping of ``{field_id: info_dict}`` built
            from the existing DataStore table's Data Dictionary, or ``None``
            if the DataStore table does not yet exist.
        :type existing_info: dict or None

        :param new_headers: the list of field dicts that will be written to
            the DataStore. Each dict has at least an ``id`` and ``type``
            key, and may include an ``info`` dict for fields that already
            existed.
        :type new_headers: list of dicts

        The return value is ignored.
        """
        pass
