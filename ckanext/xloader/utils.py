from collections import defaultdict

import ckan.plugins as p


def resource_data(id, resource_id):

    if p.toolkit.request.method == "POST":
        try:
            p.toolkit.get_action("xloader_submit")(
                None,
                {
                    "resource_id": resource_id,
                    "ignore_hash": True,  # user clicked the reload button
                },
            )
        except p.toolkit.ValidationError:
            pass

        return p.toolkit.redirect_to(
            "xloader.resource_data", id=id, resource_id=resource_id
        )

    try:
        pkg_dict = p.toolkit.get_action("package_show")(None, {"id": id})
        resource = p.toolkit.get_action("resource_show")(None, {"id": resource_id})
    except (p.toolkit.ObjectNotFound, p.toolkit.NotAuthorized):
        return p.toolkit.abort(404, p.toolkit._("Resource not found"))

    try:
        xloader_status = p.toolkit.get_action("xloader_status")(
            None, {"resource_id": resource_id}
        )
    except p.toolkit.ObjectNotFound:
        xloader_status = {}
    except p.toolkit.NotAuthorized:
        return p.toolkit.abort(403, p.toolkit._("Not authorized to see this page"))

    return p.toolkit.render(
        "xloader/resource_data.html",
        extra_vars={
            "status": xloader_status,
            "resource": resource,
            "pkg_dict": pkg_dict,
        },
    )


def get_xloader_user_apitoken():
    """ Returns the API Token for authentication.

    xloader actions require an authenticated user to perform the actions. This
    method returns the api_token set in the config file and defaults to the
    site_user.
    """
    api_token = p.toolkit.config.get('ckanext.xloader.api_token', None)
    if api_token:
        return api_token

    site_user = p.toolkit.get_action('get_site_user')({'ignore_auth': True}, {})
    return site_user["apikey"]


def column_count_modal(rows):
    """ Return the modal value of columns in the row_set's
    sample. This can be assumed to be the number of columns
    of the table.

    Copied from messytables.
    """
    counts = defaultdict(int)
    for row in rows:
        length = len([c for c in row if c != ''])
        if length > 1:
            counts[length] += 1
    if not len(counts):
        return 0
    return max(list(counts.items()), key=lambda k_v: k_v[1])[0]


def headers_guess(rows, tolerance=1):
    """ Guess the offset and names of the headers of the row set.
    This will attempt to locate the first row within ``tolerance``
    of the mode of the number of rows in the row set sample.

    The return value is a tuple of the offset of the header row
    and the names of the columns.

    Copied from messytables.
    """
    rows = list(rows)
    modal = column_count_modal(rows)
    for i, row in enumerate(rows):
        length = len([c for c in row if c != ''])
        if length >= modal - tolerance:
            # TODO: use type guessing to check that this row has
            # strings and does not conform to the type schema of
            # the table.
            return i, row
    return 0, []


def type_guess(rows, types=TYPES, strict=False):
    """ The type guesser aggregates the number of successful
    conversions of each column to each type, weights them by a
    fixed type priority and select the most probable type for
    each column based on that figure. It returns a list of
    ``CellType``. Empty cells are ignored.

    Strict means that a type will not be guessed
    if parsing fails for a single cell in the column."""
    guesses = []
    type_instances = [i for t in types for i in t.instances()]
    if strict:
        at_least_one_value = []
        for ri, row in enumerate(rows):
            diff = len(row) - len(guesses)
            for _ in range(diff):
                typesdict = {}
                for type in type_instances:
                    typesdict[type] = 0
                guesses.append(typesdict)
                at_least_one_value.append(False)
            for ci, cell in enumerate(row):
                if not cell.value:
                    continue
                at_least_one_value[ci] = True
                for type in list(guesses[ci].keys()):
                    if not type.test(cell.value):
                        guesses[ci].pop(type)
        # no need to set guessing weights before this
        # because we only accept a type if it never fails
        for i, guess in enumerate(guesses):
            for type in guess:
                guesses[i][type] = type.guessing_weight
        # in case there were no values at all in the column,
        # we just set the guessed type to string
        for i, v in enumerate(at_least_one_value):
            if not v:
                guesses[i] = {StringType(): 0}
    else:
        for i, row in enumerate(rows):
            diff = len(row) - len(guesses)
            for _ in range(diff):
                guesses.append(defaultdict(int))
            for i, cell in enumerate(row):
                # add string guess so that we have at least one guess
                guesses[i][StringType()] = guesses[i].get(StringType(), 0)
                if not cell.value:
                    continue
                for type in type_instances:
                    if type.test(cell.value):
                        guesses[i][type] += type.guessing_weight
        _columns = []
    _columns = []
    for guess in guesses:
        # this first creates an array of tuples because we want the types to be
        # sorted. Even though it is not specified, python chooses the first
        # element in case of a tie
        # See: http://stackoverflow.com/a/6783101/214950
        guesses_tuples = [(t, guess[t]) for t in type_instances if t in guess]
        _columns.append(max(guesses_tuples, key=lambda t_n: t_n[1])[0])
    return _columns
