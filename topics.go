package main

func BindingKeyForRoom(room string) string {
	return "rooms." + room
}

func BindingKeyForPrivateMsg(recipient string) string{
	return "private." + recipient
}
const BindingKeyForGlobalRoom = "global"
