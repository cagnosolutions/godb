package godb

type index struct {
	tree *btree
	ngin *engine
}

/*
   possibly broadcast tree over channel for maerge, split, underflow, and overflow
   listen to channel in index
   doesn't make sense to shrink file
   engine should grow with nodes  MATH: currentSize + (page * order) // ie. 4096 * 128 = 524288
   possible add index count
*/

func loadIndex(path string) *index {
	return &index{
		tree: NewBTree(),
		ngin: OpenEngine(path),
	}
}

func dropIndex() {
	// destoy tree and properly flush and close engine
}

func (i *index) add(k key_t, v []byte) error {
	return nil
}

func (i *index) set(k key_t, v []byte) error {
	return nil
}

func (i *index) get(k key_t) (*record, error) {
	return &record{}, nil
}

func (i *index) del(k key_t) error {
	return nil
}
