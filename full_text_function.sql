-- _full_text fields are now updated by a trigger when set to NULL
-- copied from https://github.com/ckan/ckan/pull/3786/files#diff-33d20faeb53559a9b8940bcb418cb5b4R75
CREATE OR REPLACE FUNCTION populate_full_text_trigger() RETURNS trigger
AS $body$
    BEGIN
        IF NEW._full_text IS NOT NULL THEN
            RETURN NEW;
        END IF;
        NEW._full_text := (
            SELECT to_tsvector(string_agg(value, ' '))
            FROM json_each_text(row_to_json(NEW.*))
            WHERE key NOT LIKE '\_%');
        RETURN NEW;
    END;
$body$ LANGUAGE plpgsql;
ALTER FUNCTION populate_full_text_trigger() OWNER TO ckan_default;
