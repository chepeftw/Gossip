package main

import (
    "os"
    "fmt"
    "net"
    "time"
    "strconv"
    "math/rand"
    "encoding/json"

    "github.com/op/go-logging"
    "github.com/chepeftw/treesiplibs"
)


// +++++++++++++++++++++++++++
// +++++++++ Go-Logging Conf
// +++++++++++++++++++++++++++
var log = logging.MustGetLogger("gossip")

var format = logging.MustStringFormatter(
    "%{level:.4s}=> %{time:0102 15:04:05.999} %{shortfile} %{message}",
)


// +++++++++ Constants
const (
    DefPort           = ":10000"
    Protocol          = "udp"
    BroadcastAddr     = "255.255.255.255"
    LocalhostAddr     = "127.0.0.1"
)

// Routing Messages FSM~ish
const (
    Init = iota
    WaitForReply
    Done
    Received
)

// +++++++++ Global vars
var myIP net.IP = net.ParseIP(LocalhostAddr)
var myLH net.IP = net.ParseIP(LocalhostAddr)
var timeout int = 200

var timer *time.Timer

//var startTime int64 = 0

//var globalNumberNodes int = 0
//var externalTimeout int = 0
//var globalCounter int = 0

//var Port = ":0"
//var PortInt = 0

// My intention is to have 0 for gossip routing and 1 for OLSR
//var routingMode = 0

var s1 = rand.NewSource(time.Now().UnixNano())
var r1 = rand.New(s1)

// +++++++++ Routing Protocol
//var routes map[string]string = make(map[string]string)
var RouterWaitRoom map[string]treesiplibs.Packet = make(map[string]treesiplibs.Packet)
var StampMachine map[string]int = make(map[string]int)
var ForwardedMessages []string = []string{}
var ReceivedMessages []string = []string{}


// +++++++++ Channels
var buffer = make(chan string)
var output = make(chan string)
var done = make(chan bool)

func StopTimer() {
    treesiplibs.StopTimeout(timer)
}

func StartTimer(stamp string) {
    timer = treesiplibs.StartTimeoutF(float32(timeout))

    <- timer.C
    js, err := json.Marshal(treesiplibs.AssembleTimeoutHello(stamp))
    treesiplibs.CheckError(err, log)
    buffer <- string(js)
    log.Debug("TimerHello Expired " + stamp)
}


func SendHello(stamp string) {
    SendMessage( treesiplibs.AssembleHello(myIP, stamp) )
    go StartTimer(stamp)
}
func SendHelloReply(payload treesiplibs.Packet) {
    SendMessage( treesiplibs.AssembleHelloReply(payload, myIP) )
}
func SendRoute(gateway net.IP, payloadIn treesiplibs.Packet) {
    SendMessage( treesiplibs.AssembleRoute(gateway, payloadIn) )
}

func SendMessage(payload treesiplibs.Packet) {
    js, err := json.Marshal(payload)
    treesiplibs.CheckError(err, log)
    output <- string(js)
}

// Function that handles the output channel
func attendOutputChannel() {
    ServerAddr,err := net.ResolveUDPAddr(Protocol, BroadcastAddr+DefPort)
    treesiplibs.CheckError(err, log)
    LocalAddr, err := net.ResolveUDPAddr(Protocol, myIP.String()+":0")
    treesiplibs.CheckError(err, log)
    Conn, err := net.DialUDP(Protocol, LocalAddr, ServerAddr)
    treesiplibs.CheckError(err, log)
    defer Conn.Close()

    for {
	j, more := <-output
	if more {
	    if Conn != nil {
		buf := []byte(j)
		_,err = Conn.Write(buf)
		log.Debug( myIP.String() + " " + j + " MESSAGE_SIZE=" + strconv.Itoa(len(buf)) )
		log.Info( myIP.String() + " SENDING_MESSAGE=1" )
		treesiplibs.CheckError(err, log)
	    }
	} else {
	    fmt.Println("closing channel")
	    done <- true
	    return
	}
    }
}


// Function that handles the buffer channel
func attendBufferChannel() {
    fsm := true
    for {
	j, more := <-buffer
	if more {
	    // First we take the json, unmarshal it to an object
	    payload := treesiplibs.Packet{}
	    json.Unmarshal([]byte(j), &payload)

	    fsm = false
	    stamp := payload.Timestamp
	    if _, ok := StampMachine[stamp]; !ok {
		StampMachine[stamp] = Init
	    }

	    switch payload.Type {
	    case treesiplibs.HelloType:
		if !eqIp( myIP, payload.Source) {
		    if treesiplibs.Contains(ForwardedMessages, stamp) {
			// a value ranging from 200ms to 400ms
			time.Sleep(time.Duration((r1.Intn(20000)+20000)/100) * time.Millisecond)
			StampMachine[stamp] = Init
		    }
		    SendHelloReply(payload)
		    log.Debug(myIP.String() + " => _HELLO to " + payload.Source.String())
		}

		break
	    case treesiplibs.HelloTimeoutType:
		if !treesiplibs.Contains(ForwardedMessages, stamp) {
		    SendHello(stamp)
		    StampMachine[stamp] = WaitForReply
		    log.Debug(myIP.String() + " => HELLO_TIMEOUT ON TIME" + stamp)
		} else {
		    log.Debug(myIP.String() + " => HELLO_TIMEOUT delayed " + stamp)
		}
		break
	    case treesiplibs.HelloReplyType:
		// If the HelloReply is for me ...
		if eqIp( myIP, payload.Destination ) {

		    // For the first hello reply, the stampMachine will be in WaitForReply, then it will move to Done
		    // BUT If its in Done and the actual destination replies and try to forward it again to him
		    // this is a special case
		    if ( StampMachine[stamp] == Done && eqIp(payload.Source, RouterWaitRoom[stamp].Destination) ) ||
			    StampMachine[stamp] == WaitForReply {
			StopTimer()
			SendRoute(payload.Source, RouterWaitRoom[stamp])
			ForwardedMessages = treesiplibs.AppendToList(ForwardedMessages, stamp)
			StampMachine[stamp] = Done

			log.Debug(myIP.String() + " => HELLO_REPLY WIN from " + payload.Source.String())
		    }

		}

		break
	    case treesiplibs.RouteByGossipType:
		if eqIp( myIP, payload.Gateway ) && !eqIp( myIP, payload.Destination ) {
			if StampMachine[stamp] == Init {

			    RouterWaitRoom[stamp] = payload
			    StampMachine[stamp] = WaitForReply
			    SendHello(stamp)

			    log.Debug(myIP.String() + " => ROUTE from " + payload.Source.String() + " to " + payload.Destination.String())
			}

		} else if eqIp( myIP, payload.Gateway ) && eqIp( myIP, payload.Destination ) {

		    if !treesiplibs.Contains(ReceivedMessages, stamp) {
			fsm = true
			StampMachine[stamp] = Received
			ReceivedMessages = treesiplibs.AppendToList(ReceivedMessages, stamp)

			log.Debug(myIP.String() + " SUCCESS ROUTE -> stamp: " + stamp + " from " + payload.Source.String() + " after " + strconv.Itoa(payload.Hops) + " hops")
			log.Debug(myIP.String() + " => " + j)
			log.Info(myIP.String() + " => SUCCESS_ROUTE=1")

		    } else if treesiplibs.Contains(ReceivedMessages, stamp) {
			log.Info(myIP.String() + " => SUCCESS_AGAIN_ROUTE=1")
		    }

		}
		break

	    // This case should only come from the FSM, not from the router
	    case treesiplibs.AggregateType:
		RouterWaitRoom[stamp] = payload
		StampMachine[stamp] = 1
		SendHello(stamp)
		break
	    }


	    // Now we start! FSM TIME! By redirecting it internally to the FSM in other port
	    if fsm {
		directMessage(payload)
	    }

	} else {
	    log.Debug("closing channel")
	    done <- true
	    return
	}

    }
}

func eqIp( a net.IP, b net.IP ) bool {
    return treesiplibs.CompareIPs(a, b)
}

func directMessage(payload treesiplibs.Packet) {
    ServerAddr,err := net.ResolveUDPAddr(Protocol, myLH.String()+":"+strconv.Itoa(payload.Port))
    treesiplibs.CheckError(err, log)
    LocalAddr, err := net.ResolveUDPAddr(Protocol, myIP.String()+":0")
    treesiplibs.CheckError(err, log)
    Conn, err := net.DialUDP(Protocol, LocalAddr, ServerAddr)
    treesiplibs.CheckError(err, log)
    defer Conn.Close()

    if Conn != nil {
	js, err := json.Marshal(payload)
	treesiplibs.CheckError(err, log)

	buf := []byte(js)
	_,err = Conn.Write(buf)
	log.Debug( myIP.String() + " MESSAGE_SIZE=" + strconv.Itoa(len(buf)) )
	log.Info( myIP.String() + " SENDING_MESSAGE=1" )
	treesiplibs.CheckError(err, log)
    }
}



func main() {

    //if nnodes := os.Getenv("NNODES"); nnodes != "" {
	//globalNumberNodes, _ = strconv.Atoi( nnodes )
    //}

    //targetSync := float64(0)
    //if tsync := os.Getenv("TARGETSYNC"); tsync != "" {
	//targetSync, _ = strconv.ParseFloat(tsync, 64)
    //}

    confPath := "/treesip/conf.yml"
    if len(os.Args[1:]) >= 1 {
	confPath = os.Args[1]
    }
    var c treesiplibs.Conf
    c.GetConf( confPath )

    targetSync := c.TargetSync


    // Logger configuration
    var logPath = "/var/log/golang/"
    if _, err := os.Stat(logPath); os.IsNotExist(err) {
	os.MkdirAll(logPath, 0777)
    }

    var logFile = logPath + "gossip.log"
    f, err := os.OpenFile(logFile, os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
    if err != nil {
	fmt.Printf("error opening file: %v", err)
    }
    defer f.Close()

    backend := logging.NewLogBackend(f, "", 0)
    backendFormatter := logging.NewBackendFormatter(backend, format)
    backendLeveled := logging.AddModuleLevel(backendFormatter)
    backendLeveled.SetLevel(logging.DEBUG, "")
    logging.SetBackend(backendLeveled)
    log.Info("")
    log.Info("------------------------------------------------------------------------")
    log.Info("")
    log.Info("Starting Gossip Routing process, waiting some time to get my own IP...")
    // ------------

    // It gives some time for the network to get configured before it gets its own IP.
    // This value should be passed as a environment variable indicating the time when
    // the simulation starts, this should be calculated by an external source so all
    // Go programs containers start at the same UnixTime.
    now := float64(time.Now().Unix())
    sleepTime := 0
    if targetSync > now {
	sleepTime = int(targetSync - now)
	log.Info("SYNC: Sync time is " + strconv.FormatFloat( targetSync, 'f', 6, 64) )
    }
    //else {
	//sleepTime = globalNumberNodes
    //}
    log.Info("SYNC: sleepTime is " + strconv.Itoa(sleepTime))
    time.Sleep(time.Second * time.Duration(sleepTime))
    // ------------

    // But first let me take a selfie, in a Go lang program is getting my own IP
    myIP = treesiplibs.SelfieIP()
    log.Info("Good to go, my ip is " + myIP.String())

    // Lets prepare a address at any address at port 10000
    ServerAddr,err := net.ResolveUDPAddr(Protocol, DefPort)
    treesiplibs.CheckError(err, log)

    // Now listen at selected port
    ServerConn, err := net.ListenUDP(Protocol, ServerAddr)
    treesiplibs.CheckError(err, log)
    defer ServerConn.Close()

    // Run the FSM! The one in charge of everything
    go attendBufferChannel()
    // Run the Output! The channel for communicating with the outside world!
    go attendOutputChannel()

    buf := make([]byte, 1024)

    for {
	n,_,err := ServerConn.ReadFromUDP(buf)
	buffer <- string(buf[0:n])
	treesiplibs.CheckError(err, log)
    }

    close(buffer)
    close(output)

    <-done
}
