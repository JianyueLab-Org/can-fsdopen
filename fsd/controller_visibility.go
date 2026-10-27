package fsd

import "fmt"

func (s *Server) syncControllerVisibility(client *Client) {
	if !client.HasPosition() {
		return
	}

	near := make(map[*Client]bool)
	s.postOffice.search(client, func(peer *Client) bool {
		if !peer.HasPosition() {
			return true
		}
		near[peer] = true
		if peer.IsATC {
			client.showController(peer)
		}
		if client.IsATC {
			peer.showController(client)
		}
		return true
	})

	client.visibleMu.Lock()
	for _, controller := range client.visibleControllers {
		if !near[controller] {
			client.hideControllerLocked(controller)
		}
	}
	client.visibleMu.Unlock()

	if client.IsATC {
		s.postOffice.all(client, func(peer *Client) bool {
			if !near[peer] {
				peer.hideController(client)
			}
			return true
		})
	}
}

func (c *Client) showController(controller *Client) {
	c.visibleMu.Lock()
	defer c.visibleMu.Unlock()
	if c.visibleControllers[controller.Callsign] == controller {
		return
	}
	if c.visibleControllers == nil {
		c.visibleControllers = make(map[string]*Client)
	}
	c.visibleControllers[controller.Callsign] = controller
	c.SendBestEffort(fmt.Sprintf("#AA%s:SERVER:%s:%s::%d:%d\r\n",
		controller.Callsign, controller.RealName, controller.CID,
		int(controller.NetworkRating), controller.ProtoRevision))
	if position := controller.atcPosition.Load(); position != nil {
		c.SendBestEffort(*position)
	}
}

func (c *Client) hideController(controller *Client) {
	c.visibleMu.Lock()
	defer c.visibleMu.Unlock()
	c.hideControllerLocked(controller)
}

func (c *Client) hideControllerLocked(controller *Client) {
	if c.visibleControllers[controller.Callsign] != controller {
		return
	}
	delete(c.visibleControllers, controller.Callsign)
	c.SendBestEffort(fmt.Sprintf("#DA%s:%s\r\n", controller.Callsign, controller.CID))
}
