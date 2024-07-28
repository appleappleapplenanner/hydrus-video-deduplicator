from __future__ import annotations

import io
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import av
from hvdaccelerators import stuff
from PIL import Image

from ..pdqhashing.pdq_types.hash256 import Hash256

if TYPE_CHECKING:
    from collections.abc import Iterator
    from fractions import Fraction
    from typing import Annotated, TypeAlias

    from .typing_utils import ValueRange

log = logging.getLogger(__name__)
log.setLevel(logging.CRITICAL)


@dataclass(slots=True)
class VpdqFeature:
    pdq_hash: Hash256  # 64 char hex string
    quality: float  # 0 to 100
    frame_number: int

    def assert_valid(self) -> VpdqFeature:
        """Checks the bounds of all the elements, throws ValueError if invalid"""
        if len(str(self.pdq_hash)) < Hash256.HASH256_HEX_NUM_NYBBLES:
            raise ValueError("invalid PDQ hash")
        if not (0 <= self.quality <= 100):
            raise ValueError("invalid VPDQ quality")
        if self.frame_number < 0:
            raise ValueError("invalid frame number")
        return self

    @staticmethod
    def from_str(serialized: str) -> VpdqFeature:
        """Convert from a string back to the class - the inverse of __str__"""
        parts = serialized.split(",")
        try:
            pdq_hex, qual_str, time_str = parts  # Wrong count = ValueError
            return VpdqFeature(Hash256.fromHexString(pdq_hex), float(qual_str), int(float(time_str))).assert_valid()
        except ValueError:
            raise ValueError(f"invalid {Vpdq.__name__} serialization: {serialized}")

    def __str__(self) -> str:
        return f"{self.pdq_hash},{self.quality},{self.frame_number}"


VpdqHash: TypeAlias = list[VpdqFeature]


class Vpdq:
    @staticmethod
    def get_video_bytes(video_file: Path | str | bytes) -> bytes:
        """Get the bytes of a video"""
        video = bytes()
        if isinstance(video_file, (Path, str)):
            if not Path(video_file).is_file():
                raise ValueError("Failed to get video file bytes. Video does not exist")

            try:
                with open(str(video_file), "rb") as file:
                    video = file.read()
            except OSError as exc:
                raise ValueError("Failed to get video file bytes. Invalid object type.") from exc
        elif isinstance(video_file, bytes):
            video = video_file
        else:
            raise ValueError("Failed to get video file bytes. Invalid object type.")

        return video

    @staticmethod
    def dedupe_features(features: VpdqHash) -> VpdqHash:
        """Filter out vpdq features with the exact same hash"""
        unique_features = set()
        ret = []
        for feature in features:
            if str(feature.pdq_hash) not in unique_features:
                ret.append(feature)
                unique_features.add(str(feature.pdq_hash))
        return ret

    @staticmethod
    def filter_features(vpdq_features: VpdqHash, threshold: Annotated[float, ValueRange(0.0, 100.0)]) -> VpdqHash:
        """Remove features that are below a certain quality threshold""" ""
        return [feature for feature in vpdq_features if feature.quality >= threshold]

    @staticmethod
    def feature_match_count(
        query_features: VpdqHash,
        target_features: VpdqHash,
        distance_tolerance: float,
    ) -> int:
        """Get the number of features that are within a threshold"""
        return sum(
            any(
                query_feature.pdq_hash.hammingDistanceLE(target_feature.pdq_hash, distance_tolerance)
                for target_feature in target_features
            )
            for query_feature in query_features
        )

    @staticmethod
    def match_hash(
        query_features: VpdqHash,
        target_features: VpdqHash,
        quality_tolerance: float = 50.0,
        distance_tolerance: float = 31.0,
    ):
        """Get the similarity of two videos by comparing their list of features"""
        query_filtered = Vpdq.filter_features(query_features, quality_tolerance)
        target_filtered = Vpdq.filter_features(target_features, quality_tolerance)

        # Avoid divide by zero
        if len(query_filtered) <= 0 or len(target_filtered) <= 0:
            return 0.0

        result = Vpdq.feature_match_count(query_filtered, target_filtered, distance_tolerance)
        return result * 100 / len(query_filtered)

    @staticmethod
    def frame_extract_pyav(video_bytes: bytes) -> Iterator[Image.Image]:
        """Extract frames from video"""
        with av.open(io.BytesIO(video_bytes), metadata_encoding="utf-8", metadata_errors="ignore") as container:
            # Check for video in video container
            video_streams = container.streams.video
            if video_streams is None or len(video_streams) < 1:
                log.error("Video stream not found.")
                raise ValueError("Video stream not found.")

            video = container.streams.video[0]
            video.thread_type = "AUTO"

            raw_average_fps: Fraction = video.average_rate
            average_fps: int = 1
            # Some videos, like small GIFs, will have a NoneType FPS
            if raw_average_fps is None or raw_average_fps < 1:
                log.warning("Average FPS is None or less than 1. Every frame will be hashed.")
            else:
                average_fps = round(raw_average_fps)

            for index, frame in enumerate(container.decode(video)):
                if index % average_fps == 0:
                    yield frame

    @staticmethod
    def computeHash(
        video_file: Path | str | bytes,
    ) -> VpdqHash:
        """Perceptually hash video from a file path or the bytes"""
        video = Vpdq.get_video_bytes(video_file)
        if video is None:
            raise ValueError

        # pdq = PDQHasher()
        features: VpdqHash = []

        hasher = None
        for second, frame in enumerate(Vpdq.frame_extract_pyav(video)):
            # TODO: This uses SO MUCH memory is hashing gets behind decoding since there will be
            #       lots of raw frames in the queue which are HUGE. Add a max size for the queue.
            #       ... or I have a memory leak :(
            im = frame.to_image()
            im.thumbnail((512, 512))
            if not hasher:
                # TODO: Fix this to get the average fps from frame_extract_pyav or a new method.
                #       Although this doesn't appear to actually do anything. Exact hashing tests pass...
                average_fps = 1
                hasher = stuff.Hasher(average_fps, im.width, im.height)
            rgb_image = im.convert("RGB")
            # result = stuff.hash_frame(rgb_image.tobytes(), im.width, im.height)
            hasher.hash_frame(rgb_image.tobytes())
            # (pdq_hash, pdq_quality) = result
            # pdq_hash = str(pdq_hash, encoding="utf-8")
            # pdq_frame = VpdqFeature(Hash256.fromHexString(pdq_hash), pdq_quality, second)
            # features.append(pdq_frame)
        features = hasher.finish()
        features = [
            VpdqFeature(Hash256.fromHexString(feature.get_hash()), feature.get_quality(), feature.get_frame_number())
            for feature in features
        ]
        deduped_features = Vpdq.dedupe_features(features)
        return deduped_features

    @staticmethod
    def is_similar(
        vpdq_features1: VpdqHash,
        vpdq_features2: VpdqHash,
        threshold: Annotated[float, ValueRange(0.0, 100.0)] = 75.0,
    ) -> tuple[bool, float]:
        """Check if video is similar by comparing their list of features
        Threshold is minimum similarity to be considered similar
        """
        similarity = Vpdq.match_hash(query_features=vpdq_features1, target_features=vpdq_features2)
        return similarity >= threshold, similarity

    @staticmethod
    def vpdq_to_json(vpdq_features: VpdqHash, *, indent: int | None = None) -> str:
        """Convert from VPDQ features to json object and return the json object as a str"""
        return json.dumps([str(f.assert_valid()) for f in vpdq_features], indent=indent)

    @staticmethod
    def json_to_vpdq(json_str: str) -> VpdqHash:
        """Load a str as a json object and convert from json object to VPDQ features"""
        return [VpdqFeature.from_str(s) for s in json.loads(json_str or "[]")]
