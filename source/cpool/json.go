package cpool

// noopUnmarshal will copy source into dst.
// this is to be used with the pgtype JSON codec
func jsonNoopUnmarshal(src []byte, dst any) error {
	v := make([]byte, len(src))
	copy(v, src)
	(*dst.(*any)) = v

	return nil
}