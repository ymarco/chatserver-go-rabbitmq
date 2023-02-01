package main

import (
	"errors"
	"strings"
)

type Cmd string

const CmdPrefix = "/"

func IsCmd(s string) bool {
	return strings.HasPrefix(s, CmdPrefix)
}
func UnserializeStrToCmd(s string) (cmd Cmd, args []string) {
	parts := strings.Fields(s)
	return Cmd(parts[0][1:]), parts[1:]
}
func (cmd Cmd) Serialize() string {
	return CmdPrefix + string(cmd)
}

const (
	CmdLogout    Cmd = "quit"
	CmdJoinRoom  Cmd = "join_room"
	CmdLeaveRoom Cmd = "leave_room"
	CmdSend      Cmd = "send"
	CmdSendRoom  Cmd = "send_room"
	CmdHelp      Cmd = "help"
	CmdWhisper   Cmd = "whisper"
)

var ErrUnknownCmd = errors.New("unknown cmd")

func formatDocStringForCmd(cmd Cmd, args, docs string) string {
	return "\t" + CmdPrefix + string(cmd) + "\t" + args + "\n\t\t" + docs

}

var helpString = strings.Join([]string{
	"Commands:",
	formatDocStringForCmd(CmdLogout, "", "Exit (same as pressing Ctrl-C)"),
	formatDocStringForCmd(CmdJoinRoom, "ROOM", "Join room to receive msgs from it"),
	formatDocStringForCmd(CmdLeaveRoom, "ROOM", "Leave room to stop receiving"),
	formatDocStringForCmd(CmdSendRoom, "ROOM", "Send msg to room (don't have to join it first)"),
	formatDocStringForCmd(CmdSend, "", "Send msg to the global room"),
	formatDocStringForCmd(CmdWhisper, "USERNAME", "Send msg to a specific user"),
}, "\n")
