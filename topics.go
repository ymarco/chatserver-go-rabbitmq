package main
type BindingKey string

func BindingKeyForRoom(room string) BindingKey {
	return BindingKey("rooms." + room)
}

func BindingKeyForPrivateMsg(recipient string) BindingKey{
	return BindingKey("private." + recipient)
}
const BindingKeyForGlobalRoom = BindingKey("global")
