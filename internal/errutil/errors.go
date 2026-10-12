package errutil

import (
	"errors"
	"fmt"
)

func New(message string) error {
	return errors.New(message)
}

func Is(err, target error) bool { return errors.Is(err, target) }

func Join(errs ...error) error { return errors.Join(errs...) }

func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

func Wrapf(err error, format string, arguments ...any) error {
	if err == nil {
		return nil
	}
	return Wrap(err, fmt.Sprintf(format, arguments...))
}

func Cause(err error) error {
	for err != nil {
		next := errors.Unwrap(err)
		if next == nil {
			return err
		}
		err = next
	}
	return nil
}
