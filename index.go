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

func (i *index) add(k, v []byte) error {
	return nil
}

func (i *index) set(k, v []byte) error {
	return nil
}

func (i *index) get(k []byte) (*record, error) {
	return &record{}, nil
}

func (i *index) del(k []byte) error {
	return nil
}
