from ckan.plugins.interfaces import Interface


class IXloader(Interface):
    """
    The IXloader interface allows plugin authors to receive notifications
    before and after a resource is submitted to the xloader service, as
    well as determining whether a resource should be submitted in can_upload

    The before_submit function, when implemented
    """

    def modify_download_request(self, url, resource, api_key, headers):
        """ Can be used to modify the http download request.
        The headers parameter is a dict which should be modified directly.

        Return value should be the modified (or unmodified) url.

        :param url: The download url
        :param resource: The dict representation of the resource to be downloaded
        :param api_key: The CKAN api key, in case authorization header needs to be modified
        :param headers: The http request headers dict, to be modified directly
        """
        return url

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
