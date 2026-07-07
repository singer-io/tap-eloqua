import unittest

from tap_eloqua.schema import (
    camel_to_snake,
    activity_type_to_stream,
    get_pk,
    get_type,
)


class TestSchemaUnit(unittest.TestCase):
    def test_camel_to_snake(self):
        self.assertEqual(camel_to_snake("EmailClickthrough"), "email_clickthrough")
        self.assertEqual(camel_to_snake("UpdatedAt"), "updated_at")

    def test_activity_type_to_stream(self):
        self.assertEqual(activity_type_to_stream("EmailOpen"), "activity_email_open")
        self.assertEqual(activity_type_to_stream("FormSubmit"), "activity_form_submit")

    def test_get_pk_known_streams(self):
        self.assertEqual(get_pk("accounts"), ["Id"])
        self.assertEqual(get_pk("assets"), ["id"])
        self.assertEqual(get_pk("visitors"), ["id"])
        self.assertEqual(get_pk("activity_email_open"), [])

    def test_get_pk_unknown_stream_defaults_to_id(self):
        self.assertEqual(get_pk("custom_unknown"), ["Id"])

    def test_get_type_date_number_and_string(self):
        date_field = {"dataType": "date", "internalName": "UpdatedAt"}
        number_field = {"dataType": "number", "internalName": "Score"}
        string_field = {"dataType": "string", "internalName": "Name"}

        self.assertEqual(get_type(date_field), (["null", "string"], "date-time"))
        self.assertEqual(get_type(number_field), (["null", "number"], None))
        self.assertEqual(get_type(string_field), (["null", "string"], None))

    def test_get_type_forces_numeric_ids_and_duration_to_string(self):
        numeric_id = {"dataType": "number", "internalName": "CampaignId"}
        duration = {"dataType": "number", "internalName": "Duration"}

        self.assertEqual(get_type(numeric_id), (["null", "string"], None))
        self.assertEqual(get_type(duration), (["null", "string"], None))


if __name__ == "__main__":
    unittest.main()
