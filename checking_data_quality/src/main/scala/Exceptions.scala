package com.cognira.checking.exceptions


class QualityException(message: String) extends Exception(message)
class CompletenessException(message: String) extends QualityException(message)
class IncorrectnessException(message: String) extends QualityException(message)