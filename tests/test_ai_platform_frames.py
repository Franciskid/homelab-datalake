"""Decoding usage_events' camera frames: a real tiny JPEG in, bytes out; anything
that is not a well-formed data URI skipped and counted, never raised.
"""

import base64

from homelab_datalake.parsing.ai_platform import decode_data_uri, extract_frames

# A genuine 1x1 black JPEG (the smallest valid JFIF file commonly used as a test
# fixture), base64-encoded. Decoding it is the point of the test -- a fake or
# truncated payload would not exercise the real base64 path.
_TINY_JPEG_B64 = (
    "/9j/4AAQSkZJRgABAQEAYABgAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0a"
    "HBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIy"
    "MjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIy/8AAEQgAAQABAwEiAAIR"
    "AQMRAf/EABQAAQAAAAAAAAAAAAAAAAAAAAj/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAA"
    "AAAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCdABmX/9k="
)
_TINY_JPEG_BYTES = base64.b64decode(_TINY_JPEG_B64)


def test_a_real_jpeg_data_uri_decodes_to_its_bytes():
    decoded = decode_data_uri(f"data:image/jpeg;base64,{_TINY_JPEG_B64}")
    assert decoded is not None
    assert decoded.data == _TINY_JPEG_BYTES
    assert decoded.content_type == "image/jpeg"


def test_a_png_data_uri_is_also_recognised():
    decoded = decode_data_uri(f"data:image/png;base64,{_TINY_JPEG_B64}")
    assert decoded is not None
    assert decoded.content_type == "image/png"


def test_not_a_string_is_not_a_frame():
    assert decode_data_uri(None) is None
    assert decode_data_uri(42) is None
    assert decode_data_uri({"images": []}) is None


def test_empty_string_is_not_a_frame():
    assert decode_data_uri("") is None


def test_a_string_that_is_not_a_data_uri_is_not_a_frame():
    assert decode_data_uri("https://example.test/clip.mp4") is None
    assert decode_data_uri("just some text") is None


def test_a_non_image_data_uri_is_not_a_frame():
    assert decode_data_uri("data:text/plain;base64,aGVsbG8=") is None


def test_a_data_uri_missing_the_base64_marker_is_not_a_frame():
    assert decode_data_uri("data:image/jpeg,not-base64-encoded") is None


def test_invalid_base64_payload_is_not_a_frame():
    assert decode_data_uri("data:image/jpeg;base64,not!!valid==base64==") is None


def test_a_syntactically_valid_but_empty_payload_is_not_a_frame():
    assert decode_data_uri("data:image/jpeg;base64,") is None


# --------------------------------------------------------------------------------
# extract_frames: the whole metadata.images -> (frames, skipped, remaining metadata)
# transform.
# --------------------------------------------------------------------------------


def test_every_valid_frame_is_decoded_in_order():
    metadata = {
        "images": [
            f"data:image/jpeg;base64,{_TINY_JPEG_B64}",
            f"data:image/png;base64,{_TINY_JPEG_B64}",
        ],
    }
    frames, skipped, remaining = extract_frames(metadata)
    assert len(frames) == 2
    assert frames[0].content_type == "image/jpeg"
    assert frames[1].content_type == "image/png"
    assert skipped == 0
    assert "images" not in remaining


def test_a_malformed_entry_is_skipped_and_counted_not_raised():
    """The exact case the task calls out: a malformed data URI in the middle of a
    day's frames must not fail the whole day."""
    metadata = {
        "images": [
            f"data:image/jpeg;base64,{_TINY_JPEG_B64}",
            "not-a-data-uri-at-all",
            None,
            "data:image/jpeg;base64,not-valid-base64!!",
            f"data:image/jpeg;base64,{_TINY_JPEG_B64}",
        ],
    }
    frames, skipped, _remaining = extract_frames(metadata)
    assert len(frames) == 2
    assert skipped == 3


def test_every_other_metadata_field_survives_including_cliplike_fields():
    metadata = {
        "images": [f"data:image/jpeg;base64,{_TINY_JPEG_B64}"],
        "clipUrl": "https://camera.example.test/clip/42.mp4",
        "cameraId": "cam-1",
        "nested": {"a": 1},
    }
    _frames, _skipped, remaining = extract_frames(metadata)
    assert remaining == {
        "clipUrl": "https://camera.example.test/clip/42.mp4",
        "cameraId": "cam-1",
        "nested": {"a": 1},
    }


def test_metadata_with_no_images_key_is_untouched_but_typed_correctly():
    metadata = {"clipUrl": "https://example.test/x.mp4"}
    frames, skipped, remaining = extract_frames(metadata)
    assert frames == []
    assert skipped == 0
    assert remaining == metadata


def test_images_that_is_not_a_list_is_treated_as_none_present():
    metadata = {"images": "not-a-list"}
    frames, skipped, remaining = extract_frames(metadata)
    assert frames == []
    assert skipped == 0
    assert remaining == {}


def test_non_dict_metadata_yields_nothing_rather_than_raising():
    for value in (None, "a string", 42, ["a", "list"]):
        frames, skipped, remaining = extract_frames(value)
        assert frames == []
        assert skipped == 0
        assert remaining == {}


def test_an_empty_images_list_decodes_to_nothing():
    frames, skipped, remaining = extract_frames({"images": []})
    assert frames == []
    assert skipped == 0
    assert remaining == {}
