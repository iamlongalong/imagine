package imagine

func TryTimes(f func() error, times int) error {
	var err error
	for i := 0; i < times; i++ {
		err = f()
		if err != nil {
			continue
		}

		return nil
	}

	return err
}
