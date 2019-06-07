package client

import (
	"context"
	"fmt"
	"github.com/gammazero/nexus/transport"
	"net/http"
	"sync"
	"time"

	"github.com/gammazero/nexus/wamp"
	"github.com/gorilla/websocket"
)

const (
	ClientRefreshIntervalDefault = 10 * time.Second
)

type ConnectionType string

const (
	ConnectionTypeWebsocket ConnectionType = "ws"
	ConnectionTypeRawSocket ConnectionType = "raw"

	ConnectionTypeDefault = ConnectionTypeWebsocket
)

func (ct ConnectionType) Valid() bool {
	return ct == "" || ct == ConnectionTypeRawSocket || ct == ConnectionTypeWebsocket
}

type BootstrapFailMode string

const (
	BootstrapFailModeContinue BootstrapFailMode = "continue"
	BootstrapFailModeKill     BootstrapFailMode = "kill"

	BootstrapFailModeDefault = BootstrapFailModeContinue
)

func (fm BootstrapFailMode) Valid() bool {
	return fm == "" || fm == BootstrapFailModeContinue || fm == BootstrapFailModeKill
}

type (
	// TopicSubscription describes a topic the client should immediately attempt to subscribe to upon successful
	// registration
	TopicSubscription struct {
		Topic   string       `json:"topic"`
		Handler EventHandler `json:"-"`
		Options wamp.Dict    `json:"options"`
	}

	TopicSubscriptionList []TopicSubscription

	// ProcedureRegistration describes a procedure the client should immediately attempt to register upon successful
	// connection
	ProcedureRegistration struct {
		Procedure string            `json:"procedure"`
		Handler   InvocationHandler `json:"-"`
		Options   wamp.Dict         `json:"options"`
	}

	ProcedureRegistrationList []ProcedureRegistration

	// JoinDetails
	JoinDetails struct {
		ClientRoles wamp.Dict `json:"clientRoles"`

		Topics     TopicSubscriptionList     `json:"topics"`
		Procedures ProcedureRegistrationList `json:"procedures"`
	}
)

// Config defines our Client
type ResilientClientConfig struct {
	Config
	// AutoJoin [optional]
	//
	// If true, the client will attempt to join the specified Realm immediately upon creation.
	AutoJoin bool `json:"autoJoin"`

	// ClientRefreshInterval [optional]
	//
	// Duration between attempting to re-create underlying client if it is prematurely terminated
	ClientRefreshInterval time.Duration `json:"clientRefreshInterval"`

	// ConnectionType [optional]
	//
	// Allows you to choose whether this client is constructed with a Raw or Web socket client.  Default is "ws"
	ConnectionType ConnectionType `json:"connectionType"`

	// BootstrapFailMode [optional] (default: "continue")
	//
	// Allows you to specify how the client behaves if it is unable to subscribe to any topics and / or register any
	// procedures you define in this config once it has successfully joined the target Realm within a server Node:
	//
	// 	- "continue" will cause the error to be logged but the client will stay connected. The bootstrap will proceed
	//  - "kill" will cause the error to be logged and will disconnect the client from the Server Node
	BootstrapFailMode BootstrapFailMode `json:"bootstrapFailMode"`

	// TopicSubscriptionList [optional]
	//
	// If defined, the client will attempt to subscribe to the provided list of Topics immediately after a successful
	// join is made to the Realm
	TopicSubscriptionList TopicSubscriptionList `json:"topicSubscriptionList"`

	// ProcedureRegisterList [optional]
	//
	// If defined, the client will attempt to register the provided list of Procedures immediately after a successful
	// join is made to the Realm
	ProcedureRegisterList ProcedureRegistrationList `json:"procedureRegistrationList"`
}

type ResilientClient struct {
	mu sync.RWMutex

	done   chan struct{}
	closed bool

	refreshInterval time.Duration

	// these track topic subscriptions and procedure registrations that we wish to have applied each time the underlying
	// client is created, either due to initialization or socket close -> open cycle
	resilientSubscriptions   map[string]TopicSubscription // topics to automatically subscribe to
	resilientSubscriptionsMu sync.Mutex
	resilientRegistrations   map[string]ProcedureRegistration // procedures to automatically register
	resilientRegistrationsMu sync.Mutex

	// underlying client configuration
	connectionType    ConnectionType    // what type of client to maintain
	bootstrapFailMode BootstrapFailMode // how we should handle bootstrap errors
	nexusConfig       Config            // nexus config
	nexusClient       *Client           // nexus client
	nexusClientClosed chan struct{}     // if there is a value here, the nexus client was closed for some external reason.
}

// NewClient will attempt to construct a new WS client to use
func NewResilientClient(config ResilientClientConfig) (*ResilientClient, error) {
	var (
		err error
	)

	// init client
	client := ResilientClient{
		done:                   make(chan struct{}),
		refreshInterval:        ClientRefreshIntervalDefault,
		resilientSubscriptions: make(map[string]TopicSubscription),
		resilientRegistrations: make(map[string]ProcedureRegistration),
		connectionType:         ConnectionTypeDefault,
		bootstrapFailMode:      BootstrapFailModeDefault,

		nexusClientClosed: make(chan struct{}),
	}

	// did they specify a conn type?
	if config.ConnectionType != "" {
		client.connectionType = config.ConnectionType
	}
	// did they specify a bootstrap fail mode?
	if config.BootstrapFailMode != "" {
		client.bootstrapFailMode = config.BootstrapFailMode
	}
	// did they specify a refresh interval?
	if config.ClientRefreshInterval != 0 {
		client.refreshInterval = config.ClientRefreshInterval
	}

	// stuff
	if l := len(config.TopicSubscriptionList); l > 0 {
		for _, sub := range config.TopicSubscriptionList {
			client.resilientSubscriptions[string(sub.Topic)] = sub
		}
	}
	if l := len(config.ProcedureRegisterList); l > 0 {
		for _, reg := range config.ProcedureRegisterList {
			client.resilientRegistrations[string(reg.Procedure)] = reg
		}
	}

	// if the AutoJoin field was true, immediately attempt "connect" procedure
	if config.AutoJoin {
		client.log(true, "NewClient() - \"AutoJoin\" is true")
		if err = client.connect(); err != nil {
			client.logf(false, "NewClient() - Error connecting: %s", err)
		} else {
			client.log(true, "NewClient() - Client connected")
		}
	}

	// TODO: this could be improved...

	// startup maintenance routine
	go client.maintainClient()

	// give the people what they want (peanut m&m's, mostly)
	return &client, nil
}

func (rc *ResilientClient) logf(debug bool, f string, v ...interface{}) {
	if rc.nexusConfig.Logger == nil || (debug && !rc.nexusConfig.Debug) {
		return
	}
	rc.nexusConfig.Logger.Printf(f, v...)
}

func (rc *ResilientClient) log(debug bool, v ...interface{}) {
	if rc.nexusConfig.Logger == nil || (debug && !rc.nexusConfig.Debug) {
		return
	}
	rc.nexusConfig.Logger.Print(v...)
}

// Subscribe will attempt to subscribe your client to the provided topic.  Each time a message is received, the provided
// event handler will be called.
func (rc *ResilientClient) Subscribe(topic string, fn EventHandler, options wamp.Dict) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Subscribe() - subscribe to %s", topic)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Subscribe() - subscribe to %s", topic)
	} else {
		err = rc.nexusClient.Subscribe(topic, fn, options)
	}
	rc.mu.RUnlock()
	return err
}

// ResilientSubscribe behaves just as Subscribe with the addition of adding this Topic to the list of topics to be
// automatically subscribed to in the event of WAMP client rebuild
func (rc *ResilientClient) ResilientSubscribe(topic string, fn EventHandler, options wamp.Dict) error {
	if err := rc.Subscribe(topic, fn, options); err != nil {
		return err
	}
	rc.resilientSubscriptionsMu.Lock()
	rc.resilientSubscriptions[topic] = TopicSubscription{
		Topic:   topic,
		Handler: fn,
		Options: options,
	}
	rc.resilientSubscriptionsMu.Unlock()
	return nil
}

// SubscriptionID will attempt to locate an existing subscription to the provided topic, returning the ID of the
// subscription.
func (rc *ResilientClient) SubscriptionID(topic string) (wamp.ID, bool) {
	var (
		id wamp.ID
		ok bool
	)
	rc.mu.RLock()
	if !rc.closed && rc.nexusClient != nil {
		id, ok = rc.nexusClient.SubscriptionID(topic)
	}
	rc.mu.RUnlock()
	return id, ok
}

// Unsubscribe will attempt to unsubscribe your client from the provided topic.
func (rc *ResilientClient) Unsubscribe(topic string) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Unsubscribe() - unsubscribe from %s", topic)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Unsubscribe() - unsubscribe from %s", topic)
	} else {
		err = rc.nexusClient.Unsubscribe(topic)
	}
	rc.mu.RUnlock()
	// TODO: should this always be attempted?  i think it makes sense to do so...
	rc.resilientSubscriptionsMu.Lock()
	delete(rc.resilientSubscriptions, topic)
	rc.resilientSubscriptionsMu.Unlock()
	return err
}

// Publish will attempt to publish a new message to the provided topic.
func (rc *ResilientClient) Publish(topic string, options wamp.Dict, args wamp.List, kwargs wamp.Dict) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Publish() - publish to %s", topic)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Publish() - publish to %s", topic)
	} else {
		err = rc.nexusClient.Publish(topic, options, args, kwargs)
	}
	rc.mu.RUnlock()
	return err
}

// Register will attempt to register a new procedure call with your WAMP realm.  Each time a client calls a procedure
// with this name, your invocation handler may be called depending on how you defined your LoadBalanceMethod in your
// initial RealmDefinition.
func (rc *ResilientClient) Register(procedure string, fn InvocationHandler, options wamp.Dict) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Register() - register procedure %s", procedure)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Register() - register procedure %s", procedure)
	} else {
		err = rc.nexusClient.Register(procedure, fn, options)
	}
	rc.mu.RUnlock()
	return err
}

// ResilientRegister behaves just as Register with the addition of adding this Procedure to the list of procedures to be
// automatically registered in the event of WAMP client rebuild
func (rc *ResilientClient) ResilientRegister(procedure string, fn InvocationHandler, options wamp.Dict) error {
	if err := rc.Register(procedure, fn, options); err != nil {
		return err
	}
	rc.resilientRegistrationsMu.Lock()
	rc.resilientRegistrations[procedure] = ProcedureRegistration{
		Procedure: procedure,
		Handler:   fn,
		Options:   options,
	}
	rc.resilientRegistrationsMu.Unlock()
	return nil
}

// RegistrationID will attempt to locate the ID of an existing procedure's registration
func (rc *ResilientClient) RegistrationID(procedure string) (wamp.ID, bool) {
	var (
		id wamp.ID
		ok bool
	)
	rc.mu.RLock()
	if !rc.closed && rc.nexusClient != nil {
		id, ok = rc.nexusClient.RegistrationID(procedure)
	}
	rc.mu.RUnlock()
	return id, ok
}

// Unregister will attempt to remove a given procedure from the WAMP Realm.  This ONLY impacts registrations done by
// THIS client.  If you have a load balanced procedure that is registered across multiple clients, they will continue
// to function
func (rc *ResilientClient) Unregister(procedure string) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Unregister() - unregister procedure %s", procedure)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Unregister() - unregister procedure %s", procedure)
	} else {
		err = rc.nexusClient.Unregister(procedure)
	}
	rc.mu.RUnlock()
	// TODO: should this always be attempted?  i think it makes sense to delete from the persistent map even if there is no connection...
	rc.resilientRegistrationsMu.Lock()
	delete(rc.resilientRegistrations, procedure)
	rc.resilientRegistrationsMu.Unlock()
	return err
}

// SendProgress allows you to send progressive results within a procedure's execution window.
func (rc *ResilientClient) SendProgress(ctx context.Context, args wamp.List, kwargs wamp.Dict) error {
	var err error
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform SendProgress() - send progress. args: %v; kwargs: %v", args, kwargs)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform SendProgress() - send progress. args: %v; kwargs: %v", args, kwargs)
	} else {
		err = rc.nexusClient.SendProgress(ctx, args, kwargs)
	}
	rc.mu.RUnlock()
	return err
}

// Call will attempt to call a previously registered procedure.
func (rc *ResilientClient) Call(ctx context.Context, procedure string, options wamp.Dict, args wamp.List, kwargs wamp.Dict, cancelMode string) (*wamp.Result, error) {
	var (
		res *wamp.Result
		err error
	)
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform Call() - call procedure %s", procedure)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform Call() - call procedure %s", procedure)
	} else {
		res, err = rc.nexusClient.Call(ctx, procedure, options, args, kwargs, cancelMode)
	}
	rc.mu.RUnlock()
	return res, err
}

// CallProgress allows you to take advantage of mid-progress updates from your registered procedure, should your
// procedure implementation support that.
//
// For each "SendProgress" call your registering service makes, the "progcb" handler provided to this method will be
// called, until such time as either an error is seen or your registering service returns a final result.
func (rc *ResilientClient) CallProgress(ctx context.Context, procedure string, options wamp.Dict, args wamp.List, kwargs wamp.Dict, cancelMode string, progcb ProgressCallback) (*wamp.Result, error) {
	var (
		res *wamp.Result
		err error
	)
	rc.mu.RLock()
	if rc.closed {
		err = fmt.Errorf("client is closed, cannot perform CallProgress() - progressively call procedure %s", procedure)
	} else if rc.nexusClient == nil {
		err = fmt.Errorf("client connection is down, cannot perform CallProgress() - progresively call procedure %s", procedure)
	} else {
		res, err = rc.nexusClient.CallProgress(ctx, procedure, options, args, kwargs, cancelMode, progcb)
	}
	rc.mu.RUnlock()
	return res, err
}

// Close will stop all credential refresh operations and, if opened, close the underlying WAMP client connection.
//
// Once closed, this client becomes defunct.
func (rc *ResilientClient) Close() error {
	rc.mu.Lock()
	if rc.closed {
		rc.mu.Unlock()
		return nil
	}

	// only possible err is nexus client close
	var err error

	// mark as closed
	rc.closed = true

	// stop the loops
	close(rc.done)

	// if we have a nexus client, attempt to close and capture error
	if rc.nexusClient != nil {
		err = rc.nexusClient.Close()
		rc.nexusClient = nil
	}

	rc.mu.Unlock()

	return err
}

func (rc *ResilientClient) createWebSocketPeer() (wamp.Peer, error) {

	// TODO: do better here...
	protocol, payloadType, serializer := rc.serializationType.WebsocketConfig()

	dialer := websocket.Dialer{
		Subprotocols:    []string{protocol},
		TLSClientConfig: rc.tlsConfig,
		Proxy:           http.ProxyFromEnvironment,
		NetDial:         nil,
	}

	conn, _, err := dialer.Dial(realmURL, nil)
	if err != nil {
		return nil, fmt.Errorf("createWebSocketPeer() - error dialing %q: %s", realmURL, err)
	}

	rc.log(true, "createWebSocketPeer() - WebSocket peer built")

	// build websocket peer
	return transport.NewWebsocketPeer(conn, serializer, payloadType, rc.nexusConfig.Logger, 5*time.Second, rc.nexusConfig.RecvLimit), nil
}

func (rc *ResilientClient) createRawSocketPeer() (wamp.Peer, error) {
	panic("createRawSocketPeer() - raw connection type is not yet implemented")
}

// handleClientClose waits on the provided client's "Done" channel to be closed.  Once closed, it will nil out the local
// nexus client.  Until the maintenance loop runs again, this will cause all calls to fail
// TODO: This could be vastly improved, probably...
func (rc *ResilientClient) handleClientClose() {
	// there is a slight race condition here if done is closed between the time the go routine is called and it being
	// run.  not a huge fan of this...
	select {
	case <-rc.done:
		return
	case <-rc.nexusClient.Done():
	}
	rc.log(false, "handleClientClose() - WAMP Connection has been terminated")
	select {
	case rc.nexusClientClosed <- struct{}{}:
	default:
		rc.log(false, "handleClientClose() - Unable to push to nexusClientClosed chan")
	}
}

// buildNexusClient performs the actual task of constructing the underlying nexus client that will actually do the
// wamp'ing
//
// caller must hold lock
func (rc *ResilientClient) buildNexusClient() error {
	var (
		peer wamp.Peer
		err  error
	)

	switch rc.connectionType {
	case ConnectionTypeWebsocket:
		peer, err = rc.createWebSocketPeer()
	case ConnectionTypeRawSocket:
		peer, err = rc.createRawSocketPeer()

	default:
		panic(fmt.Sprintf("buildNexusClient() - unknown connection type \"%s\" seen", rc.connectionType))
	}

	if err != nil {
		return fmt.Errorf("buildNexusClient() - error building %s peer: %s", rc.connectionType, err)
	}

	rc.log(true, "buildNexusClient() - Peer built")

	if rc.nexusClient, err = NewClient(peer, rc.nexusConfig); err != nil {
		return fmt.Errorf("buildNexusClient() - unable to construct Nexus client: %s", err)
	}

	// spin up go routine to watch for client closure
	go rc.handleClientClose()

	return nil
}

func (rc *ResilientClient) bootstrapClient() error {
	rlen := len(rc.resilientSubscriptions)
	plen := len(rc.resilientRegistrations)

	if rlen == 0 && plen == 0 {
		rc.log(true, "bootstrapClient() - No bootstrap subscriptions or registrations defined")
		return nil
	}

	rc.logf(true, "bootstrapClient() - Bootstrapping client with: %d subscriptions; %d registrations", rlen, plen)

	for _, sub := range rc.resilientSubscriptions {
		if err := rc.nexusClient.Subscribe(string(sub.Topic), sub.Handler, sub.Options); err != nil {
			rc.logf(false, "bootstrapClient() - Unable to subscribe to %q: %s", sub.Topic, err)
			if rc.bootstrapFailMode == BootstrapFailModeKill {
				return err
			}
			rc.log(false, "bootstrapClient() - Continuing with bootstrap")
		} else {
			rc.logf(true, "bootstrapClient() - Subscribed to topic %q", sub.Topic)
		}
	}

	for _, proc := range rc.resilientRegistrations {
		if err := rc.nexusClient.Register(string(proc.Procedure), proc.Handler, proc.Options); err != nil {
			rc.logf(false, fmt.Sprintf("bootstrapClient() - Unable to register %q: %s", proc.Procedure, err))
			if rc.bootstrapFailMode == BootstrapFailModeKill {
				return err
			}
			rc.log(false, "bootstrapClient() - Continuing with bootstrap")
		} else {
			rc.logf(true, "bootstrapClient() - Registered procedure %q", proc.Procedure)
		}
	}

	return nil
}

func (rc *ResilientClient) connect() error {
	// next, attempt to build client
	if err := rc.buildNexusClient(); err != nil {
		return err
	}

	// attempt to bootstrap client
	if err := rc.bootstrapClient(); err != nil {
		return err
	}

	return nil
}

// maintainClient is designed to run for the entire duration of the client's existence.  It can only be stopped by
// calling .Close() in the client.
func (rc *ResilientClient) maintainClient() {
	rc.log(true, "maintainClient() - Running")

	var (
		tick time.Time

		clientRefreshTimer = time.NewTimer(rc.refreshInterval)
	)

	defer func() {
		rc.log(false, "maintainClient() - Stopping")

		// nil out client
		rc.mu.Lock()
		if rc.nexusClient != nil {
			if err := rc.nexusClient.Close(); err != nil {
				rc.logf(false, "maintainClient() - Error closing nexus client: %s", err)
			}
		}
		rc.mu.Unlock()

		close(rc.nexusClientClosed)
		if len(rc.nexusClientClosed) > 0 {
			for range rc.nexusClientClosed {
			}
		}

		// stop client timer
		clientRefreshTimer.Stop()
	}()

	for {
		select {
		case tick = <-clientRefreshTimer.C:
			rc.logf(true, "maintainClient() - clientRefreshTimer hit: %s", tick)

			// client refresh loop
			rc.mu.RLock()
			if rc.nexusClient == nil {
				rc.mu.RUnlock()
				rc.mu.Lock()
				rc.log(false, "maintainClient() - Nexus Client is nil, will attempt to reconnect")
				if err := rc.connect(); err != nil {
					rc.logf(false, "maintainClient() - Unable to rebuild Nexus Client: %s", err)
				} else {
					rc.log(true, "maintainClient() - Nexus Client rebuilt successfully")
				}
				rc.mu.Unlock()
			} else {
				rc.mu.RUnlock()
			}

			clientRefreshTimer.Reset(rc.refreshInterval)

		case <-rc.nexusClientClosed:
			rc.log(true, "maintainClient() - nexusClientClosed hit")
			// if the underlying client was closed for some reason
			rc.mu.Lock()
			rc.nexusClient = nil
			rc.mu.Unlock()

			// always try to stop the timer
			if !clientRefreshTimer.Stop() {
				<-clientRefreshTimer.C
			}

			clientRefreshTimer.Reset(rc.refreshInterval)

		case <-rc.done:
			rc.log(false, "maintainClient() - done chan closed, will exit loop")
			return
		}
	}
}
