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

    def datastore_before_update(self, resource_id, existing_fields, new_headers):
        """ Called by the loader just before it is about to modify the
        DataStore table for a resource (truncate, drop+recreate, or create).
        It allows plugins to inspect the difference between the current
        DataStore columns and the ones detected in the incoming file, for
        example to log an activity when columns are added, removed or
        renamed.

        Both ``existing_fields`` and ``new_headers`` are lists of field
        dicts that contain at least an ``id`` key, so a plugin can compute
        the diff symmetrically::

            old_ids = {f['id'] for f in existing_fields or []}
            new_ids = {h['id'] for h in new_headers}
            added = new_ids - old_ids
            removed = old_ids - new_ids

        :param resource_id: the ID of the resource whose DataStore table is
            about to be updated.
        :type resource_id: string

        :param existing_fields: the current columns of the DataStore table
            (the internal ``_id`` column is excluded), or ``None`` if the
            DataStore table does not yet exist. Each dict contains at least
            ``id`` and ``type`` and may include ``info`` for fields with a
            Data Dictionary entry.
        :type existing_fields: list of dicts or None

        :param new_headers: the list of field dicts that will be written to
            the DataStore. Each dict has at least an ``id`` and ``type``
            key, and may include an ``info`` dict for fields that already
            existed.
        :type new_headers: list of dicts

        .. warning::

            The ``existing_fields`` and ``new_headers`` lists are the
            same objects that the loader will use after this hook returns.
            Mutating them (e.g. adding, removing or renaming fields) will
            affect the subsequent DataStore operation.  This hook is
            intended for **read-only observation**; modify the lists only
            if you fully understand the downstream consequences.

        The return value is ignored.
        """
        pass
