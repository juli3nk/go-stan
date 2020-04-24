package publish

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/stan.go"
)

func Publish(URL, clusterID, clientID, subject string, msg []byte, async bool) error {
	sc, err := stan.Connect(clusterID, clientID, stan.NatsURL(URL))
	if err != nil {
		return fmt.Errorf("Can't connect: %v.\nMake sure a NATS Streaming Server is running at: %s", err, URL)
	}
	defer sc.Close()

	ch := make(chan bool)
	var glock sync.Mutex
	var guid string
	acb := func(lguid string, err error) {
		glock.Lock()
		log.Printf("Received ACK for guid %s\n", lguid)
		defer glock.Unlock()
		if err != nil {
			fmt.Errorf("Error in server ack for guid %s: %v\n", lguid, err)
		}
		if lguid != guid {
			fmt.Errorf("Expected a matching guid in ack callback, got %s vs %s\n", lguid, guid)
		}
		ch <- true
	}

	if !async {
		err = sc.Publish(subject, msg)
		if err != nil {
			return fmt.Errorf("Error during publish: %v\n", err)
		}
		log.Printf("Published [%s] : '%s'\n", subject, msg)
	} else {
		glock.Lock()
		guid, err = sc.PublishAsync(subject, msg, acb)
		if err != nil {
			return fmt.Errorf("Error during async publish: %v\n", err)
		}
		glock.Unlock()
		if guid == "" {
			return fmt.Errorf("Expected non-empty guid to be returned.")
		}
		log.Printf("Published [%s] : '%s' [guid: %s]\n", subject, msg, guid)

		select {
		case <-ch:
			break
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout")
		}

	}

	return nil
}
