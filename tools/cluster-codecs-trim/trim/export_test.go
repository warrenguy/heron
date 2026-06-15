package trim

func FindChildForTest(root Element, name string) (Element, bool) {
	return findChild(root, name)
}

func AttrValueForTest(elem Element, key string) string {
	return attrValue(elem, key)
}
