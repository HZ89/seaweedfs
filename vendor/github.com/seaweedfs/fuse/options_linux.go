package fuse

func autoXattr(conf *mountConfig) error {
	return nil
}

func localVolume(conf *mountConfig) error {
	return nil
}

func volumeName(name string) MountOption {
	return dummyOption
}

func daemonTimeout(name string) MountOption {
	return dummyOption
}

func noAppleXattr(conf *mountConfig) error {
	return nil
}

func noAppleDouble(conf *mountConfig) error {
	return nil
}

func exclCreate(conf *mountConfig) error {
	return nil
}

func noBrowse(conf *mountConfig) error {
	return nil
}
