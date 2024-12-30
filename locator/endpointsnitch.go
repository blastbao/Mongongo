package locator

import (
	"log"
	"net"

	"github.com/DistAlchemist/Mongongo/network"
)

// EndPointSnitch try to infer the location of an endpoint
// such as rack or datacenter information.
type EndPointSnitch struct {
	//
}

// IsOnSameRack checks whether two hosts are on the same
// rack by comparing ip addr's 3rd octet.
//
// 判断两个主机是否在同一个机架上。
func IsOnSameRack(host, host2 network.EndPoint) bool {
	// Compare the 3rd octet.
	// if ==, on same rack
	// else not
	ip := getIPAddr(host.HostName)
	ip2 := getIPAddr(host2.HostName)
	return ip[2] == ip2[2]
}

// IsOnSameDataCenter checks whether two hosts are on the same
// datacenter by comparing ip addr's 2nd octet.
//
// 判断两个主机是否在同一个数据中心。
func IsOnSameDataCenter(host, host2 network.EndPoint) bool {
	ip := getIPAddr(host.HostName)
	ip2 := getIPAddr(host2.HostName)
	return ip[1] == ip2[1]
}

// 获取 ip 地址
func getIPAddr(host string) []byte {
	addrs, err := net.LookupHost(host)
	if err != nil {
		log.Printf("error when looking up host %v: %v\n", host, err)
	}
	return []byte(addrs[0])
}
