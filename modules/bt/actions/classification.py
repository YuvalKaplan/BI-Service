from modules.bt.object import categorize_ticker as _ct
from modules.calc.classification import (
    StyleClassifier,
    update_factor_cache as _update_factor_cache,
    train_model as _train_model,
    get_classifier as _get_classifier,
)


def update_factor_cache() -> None:
    _update_factor_cache(_ct)


def train_model():
    return _train_model(_ct)


def get_classifier(update_training_set: bool) -> StyleClassifier:
    return _get_classifier(_ct, update_training_set)
