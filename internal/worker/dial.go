package worker

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"time"
)

type socketDial func(context.Context, string, string) (net.Conn, error)
type privateDialer struct {
	DNS  DNS
	Dial socketDial
}

func (d privateDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	host, port, e := net.SplitHostPort(address)
	p, pe := strconv.Atoi(port)
	if e != nil || pe != nil || p < 1 || p > 65535 {
		return nil, errors.New("invalid private peer")
	}
	if d.DNS == nil {
		d.DNS = net.DefaultResolver
	}
	ips, e := d.DNS.LookupHost(ctx, host)
	if e != nil || len(ips) == 0 {
		return nil, errors.New("private peer DNS failed")
	}
	for _, ip := range ips {
		a, e := netip.ParseAddr(ip)
		if e != nil || !a.IsPrivate() {
			return nil, errors.New("peer must resolve exclusively to private addresses")
		}
	}
	dial := d.Dial
	if dial == nil {
		dial = (&net.Dialer{Timeout: 5 * time.Second}).DialContext
	}
	for _, ip := range ips {
		c, e := dial(ctx, network, net.JoinHostPort(ip, port))
		if e == nil {
			return c, nil
		}
		if ctx.Err() != nil {
			break
		}
	}
	return nil, errors.New("private peer connection failed")
}
