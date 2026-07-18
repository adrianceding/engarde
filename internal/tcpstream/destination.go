package tcpstream

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

type DestinationType uint8

const (
	DestinationIPv4   DestinationType = 0x01
	DestinationDomain DestinationType = 0x03
	DestinationIPv6   DestinationType = 0x04
)

var ErrInvalidDestination = errors.New("invalid TCP destination")

type Destination struct {
	typeCode DestinationType
	host     string
	port     uint16
}

func ParseDestination(address string) (Destination, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return Destination{}, fmt.Errorf("%w: %q", ErrInvalidDestination, address)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return Destination{}, fmt.Errorf("%w: port %q", ErrInvalidDestination, portText)
	}
	return NewDestination(host, uint16(port))
}

func NewDestination(host string, port uint16) (Destination, error) {
	if host == "" || host != strings.TrimSpace(host) || port == 0 || strings.Contains(host, "%") {
		return Destination{}, ErrInvalidDestination
	}
	if address, err := netip.ParseAddr(host); err == nil {
		address = address.Unmap()
		if address.Is4() {
			return Destination{typeCode: DestinationIPv4, host: address.String(), port: port}, nil
		}
		return Destination{typeCode: DestinationIPv6, host: address.String(), port: port}, nil
	}

	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if !validDomain(host) {
		return Destination{}, ErrInvalidDestination
	}
	return Destination{typeCode: DestinationDomain, host: host, port: port}, nil
}

func DecodeDestination(payload []byte) (Destination, error) {
	if len(payload) < 1+2 {
		return Destination{}, ErrInvalidDestination
	}
	var host string
	var portOffset int
	switch DestinationType(payload[0]) {
	case DestinationIPv4:
		if len(payload) != 1+4+2 {
			return Destination{}, ErrInvalidDestination
		}
		address, ok := netip.AddrFromSlice(payload[1:5])
		if !ok {
			return Destination{}, ErrInvalidDestination
		}
		host = address.String()
		portOffset = 5
	case DestinationIPv6:
		if len(payload) != 1+16+2 {
			return Destination{}, ErrInvalidDestination
		}
		address, ok := netip.AddrFromSlice(payload[1:17])
		if !ok {
			return Destination{}, ErrInvalidDestination
		}
		host = address.String()
		portOffset = 17
	case DestinationDomain:
		if len(payload) < 1+1+1+2 {
			return Destination{}, ErrInvalidDestination
		}
		length := int(payload[1])
		if length == 0 || len(payload) != 1+1+length+2 {
			return Destination{}, ErrInvalidDestination
		}
		host = string(payload[2 : 2+length])
		portOffset = 2 + length
	default:
		return Destination{}, ErrInvalidDestination
	}
	return NewDestination(host, binary.BigEndian.Uint16(payload[portOffset:]))
}

func (destination Destination) Encode() ([]byte, error) {
	if destination.IsZero() {
		return nil, ErrInvalidDestination
	}
	var payload []byte
	switch destination.typeCode {
	case DestinationIPv4:
		address, err := netip.ParseAddr(destination.host)
		if err != nil || !address.Is4() {
			return nil, ErrInvalidDestination
		}
		value := address.As4()
		payload = make([]byte, 1+len(value)+2)
		payload[0] = byte(DestinationIPv4)
		copy(payload[1:], value[:])
	case DestinationIPv6:
		address, err := netip.ParseAddr(destination.host)
		if err != nil || !address.Is6() || address.Is4In6() {
			return nil, ErrInvalidDestination
		}
		value := address.As16()
		payload = make([]byte, 1+len(value)+2)
		payload[0] = byte(DestinationIPv6)
		copy(payload[1:], value[:])
	case DestinationDomain:
		if !validDomain(destination.host) || len(destination.host) > 255 {
			return nil, ErrInvalidDestination
		}
		payload = make([]byte, 1+1+len(destination.host)+2)
		payload[0] = byte(DestinationDomain)
		payload[1] = byte(len(destination.host))
		copy(payload[2:], destination.host)
	default:
		return nil, ErrInvalidDestination
	}
	binary.BigEndian.PutUint16(payload[len(payload)-2:], destination.port)
	return payload, nil
}

func (destination Destination) IsZero() bool {
	return destination.typeCode == 0 || destination.host == "" || destination.port == 0
}

func (destination Destination) Type() DestinationType {
	return destination.typeCode
}

func (destination Destination) Host() string {
	return destination.host
}

func (destination Destination) Port() uint16 {
	return destination.port
}

func (destination Destination) String() string {
	if destination.IsZero() {
		return ""
	}
	return net.JoinHostPort(destination.host, strconv.FormatUint(uint64(destination.port), 10))
}

func validDomain(host string) bool {
	if host == "" || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}
