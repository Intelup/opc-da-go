//go:build windows
// +build windows

package opc

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	ole "github.com/go-ole/go-ole"
	"github.com/go-ole/go-ole/oleutil"
	"golang.org/x/sys/windows/registry"
)

func init() {
	OleInit()
}

// OleInit initializes OLE.
func OleInit() {
	ole.CoInitializeEx(0, 0)
}

// OleRelease releases OLE resources in opcAutomation.
func OleRelease() {
	ole.CoUninitialize()
}

// AutomationObject loads the OPC Automation Wrapper and handles the connection to the OPC Server.
type AutomationObject struct {
	unknown *ole.IUnknown
	object  *ole.IDispatch
}

// CreateBrowser initializes the OPCBrowser and returns the root or specific branch based on the nextName parameter.
func (ao *AutomationObject) CreateBrowser(path string) (*Tree, error) {
	if !ao.IsConnected() {
		return nil, errors.New("cannot create browser because we are not connected")
	}

	if ao.object == nil {
		return nil, errors.New("ao.object is nil, cannot create browser")
	}

	browser, err := oleutil.CallMethod(ao.object, "CreateBrowser")
	if err != nil {
		return nil, errors.New("failed to create OPCBrowser")
	}

	bd := browser.ToIDispatch()
	if bd == nil {
		return nil, errors.New("CreateBrowser returned nil browser")
	}

	// Move to the root of the tree
	_, err = oleutil.CallMethod(bd, "MoveToRoot")
	if err != nil {
		return nil, errors.New("failed to MoveToRoot in browser")
	}

	// Navigate through the path
	segments := strings.Split(path, "/")
	for _, segment := range segments {
		if segment == "" {
			continue
		}

		logger.Printf("Navigating to branch: %s", segment)

		_, err := oleutil.CallMethod(bd, "MoveDown", segment)
		if err != nil {
			return nil, fmt.Errorf("failed to navigate to branch '%s': %v", segment, err)
		}
	}

	// Create and populate the tree at the final branch
	branch := Tree{Name: segments[len(segments)-1], Parent: nil, Branches: []*Tree{}, Leaves: []Leaf{}}
	err = buildBranch(bd, &branch)
	if err != nil {
		bd.Release()
		return nil, err
	}
	bd.Release()
	return &branch, nil
}

// buildBranch populates the Tree structure for a single branch.
func buildBranch(browser *ole.IDispatch, branch *Tree) error {
	if browser == nil {
		return errors.New("browser is nil in buildBranch")
	}

	// Get and add leaves
	res := oleutil.MustCallMethod(browser, "ShowLeafs")
	if res.VT == ole.VT_DISPATCH && res.Value() == nil {
		logger.Println("ShowLeafs returned nil")
	}
	countVar := oleutil.MustGetProperty(browser, "Count")
	if countVar.VT != ole.VT_I4 {
		return errors.New("Count property is not int32")
	}
	count := countVar.Value().(int32)

	logger.Println("\tLeafs count:", count)
	for i := 1; i <= int(count); i++ {
		itemRes := oleutil.MustCallMethod(browser, "Item", i)
		if itemRes == nil || itemRes.VT == ole.VT_EMPTY {
			logger.Println("Item call returned nil or empty for index", i)
			continue
		}
		item := itemRes.Value()

		tagRes := oleutil.MustCallMethod(browser, "GetItemID", item)
		if tagRes == nil || tagRes.VT == ole.VT_EMPTY {
			logger.Println("GetItemID returned nil or empty for item", item)
			continue
		}
		tag := tagRes.Value()

		l := Leaf{Name: fmt.Sprintf("%v", item), Tag: fmt.Sprintf("%v", tag)}
		branch.Leaves = append(branch.Leaves, l)
	}

	// Get branches
	oleutil.MustCallMethod(browser, "ShowBranches")
	countVar = oleutil.MustGetProperty(browser, "Count")
	if countVar.VT != ole.VT_I4 {
		return errors.New("Count property is not int32 in branches")
	}
	count = countVar.Value().(int32)

	logger.Println("\tBranches count:", count)
	for i := 1; i <= int(count); i++ {
		nextNameRes := oleutil.MustCallMethod(browser, "Item", i)
		if nextNameRes == nil || nextNameRes.VT == ole.VT_EMPTY {
			logger.Println("Item call returned nil for branch index", i)
			continue
		}
		nextName := nextNameRes.Value()

		logger.Println("\t", i, "next branch:", nextName)
		branch.Branches = append(branch.Branches, &Tree{
			Name:     fmt.Sprintf("%v", nextName),
			Parent:   branch,
			Branches: []*Tree{},
			Leaves:   []Leaf{},
		})
	}

	return nil
}

// Connect establishes a connection to the OPC Server on node.
func (ao *AutomationObject) Connect(server string, node string) (*AutomationItems, error) {
	ao.disconnect()

	if ao.object == nil {
		return nil, errors.New("ao.object is nil, cannot connect")
	}

	logger.Printf("Connecting to %s on node %s\n", server, node)
	_, err := oleutil.CallMethod(ao.object, "Connect", server, node)
	if err != nil {
		logger.Println("Connection failed.")
		return nil, errors.New("connection failed")
	}

	opcGroupsVar, err := oleutil.GetProperty(ao.object, "OPCGroups")
	if err != nil {
		return nil, errors.New("cannot get OPCGroups property")
	}
	opcGroups := opcGroupsVar.ToIDispatch()
	if opcGroups == nil {
		return nil, errors.New("OPCGroups is nil")
	}

	opcGrpVar, err := oleutil.CallMethod(opcGroups, "Add")
	if err != nil {
		return nil, errors.New("cannot add new OPC Group")
	}
	opcGrp := opcGrpVar.ToIDispatch()
	if opcGrp == nil {
		opcGroups.Release()
		return nil, errors.New("OPCGroup is nil after add")
	}

	addItemObjectVar, err := oleutil.GetProperty(opcGrp, "OPCItems")
	if err != nil {
		opcGrp.Release()
		opcGroups.Release()
		return nil, errors.New("cannot get OPC Items")
	}
	addItemObject := addItemObjectVar.ToIDispatch()
	if addItemObject == nil {
		opcGrp.Release()
		opcGroups.Release()
		return nil, errors.New("OPCItems is nil")
	}

	// Aqui liberamos opcGroups e opcGrp porque não serão mais usados
	opcGrp.Release()
	opcGroups.Release()

	logger.Println("Connected.")

	return NewAutomationItems(addItemObject), nil
}

// TryConnect tries to connect to any of the nodes.
func (ao *AutomationObject) TryConnect(server string, nodes []string) (*AutomationItems, error) {
	var errResult string
	for _, node := range nodes {
		items, err := ao.Connect(server, node)
		if err == nil {
			return items, nil
		}
		errResult = errResult + err.Error() + "\n"
	}
	return nil, errors.New("TryConnect was not successful: " + errResult)
}

// IsConnected checks if the server is properly connected.
func (ao *AutomationObject) IsConnected() bool {
	if ao.object == nil {
		return false
	}
	stateVt, err := oleutil.GetProperty(ao.object, "ServerState")
	if err != nil {
		logger.Println("GetProperty call for ServerState failed", err)
		return false
	}
	val, ok := stateVt.Value().(int32)
	if !ok {
		return false
	}
	if val != OPCRunning {
		return false
	}
	return true
}

// GetOPCServers returns a list of ProgIDs on the specified node
func (ao *AutomationObject) GetOPCServers(node string) []string {
	if ao.object == nil {
		logger.Println("ao.object is nil in GetOPCServers")
		return []string{}
	}
	progids, err := oleutil.CallMethod(ao.object, "GetOPCServers", node)
	if err != nil {
		logger.Println("GetOPCServers call failed.")
		return []string{}
	}

	arr := progids.ToArray()
	if arr == nil {
		return []string{}
	}

	var servers_found []string
	for _, v := range arr.ToStringArray() {
		if v != "" {
			servers_found = append(servers_found, v)
		}
	}
	return servers_found
}

// disconnect checks if connected and disconnects if so.
func (ao *AutomationObject) disconnect() {
	if ao.IsConnected() && ao.object != nil {
		_, err := oleutil.CallMethod(ao.object, "Disconnect")
		if err != nil {
			logger.Println("Failed to disconnect. %v", err.Error())
		}
	}
}

// Close releases the OLE objects in the AutomationObject.
func (ao *AutomationObject) Close() {
	if ao.object != nil {
		ao.disconnect()
		ao.object.Release()
		ao.object = nil
	}
	if ao.unknown != nil {
		ao.unknown.Release()
		ao.unknown = nil
	}
}

func FindOPCWrappers() ([]string, error) {
	var availableWrappers []string

	// Verifica no registro HKEY_CLASSES_ROOT
	regKey := `Software\Classes`
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, regKey, registry.READ)
	if err != nil {
		return nil, err
	}
	defer k.Close()

	// Itera pelas chaves do registro
	subKeys, err := k.ReadSubKeyNames(0)
	if err != nil {
		return nil, err
	}

	// Busca por objetos COM que possivelmente são wrappers OPC
	for _, subKey := range subKeys {
		// Verifica se o nome da chave contém a palavra "OPC"
		if strings.Contains(subKey, "OPC") {
			availableWrappers = append(availableWrappers, subKey)
			logger.Println("Wrapper OPC encontrado: %s", subKey)
		}
	}

	return availableWrappers, nil
}

// NewAutomationObject connects to the COM object based on available wrappers.
func NewAutomationObject() *AutomationObject {
	wrappers, err := FindOPCWrappers()

	if err != nil {
		logger.Fatalf("Erro ao buscar wrappers OPC: %v", err)
	}

	var unknown *ole.IUnknown
	for _, wrapper := range wrappers {
		unknown, err = oleutil.CreateObject(wrapper)
		if err == nil && unknown != nil {
			logger.Println("Loaded OPC Automation object with wrapper", wrapper)
			opc, qErr := unknown.QueryInterface(ole.IID_IDispatch)
			if qErr == nil && opc != nil {
				return &AutomationObject{
					unknown: unknown,
					object:  opc,
				}
			} else {
				if opc != nil {
					opc.Release()
				}
				if unknown != nil {
					unknown.Release()
				}
			}
		} else {
			logger.Println("Could not load OPC Automation object with wrapper", wrapper)
		}
	}
	return &AutomationObject{}
}

// AutomationItems store the OPCItems.
type AutomationItems struct {
	addItemObject *ole.IDispatch
	items         map[string]*ole.IDispatch
}

func (ai *AutomationItems) addSingle(tag string) error {
	if ai.addItemObject == nil {
		return errors.New("addItemObject is nil, cannot add item")
	}
	clientHandle := int32(1)
	item, err := oleutil.CallMethod(ai.addItemObject, "AddItem", tag, clientHandle)
	if err != nil {
		return errors.New(tag + ":" + err.Error())
	}
	idisp := item.ToIDispatch()
	if idisp == nil {
		return errors.New("AddItem returned nil for tag " + tag)
	}
	ai.items[tag] = idisp
	return nil
}

type ErrResult struct {
	Message string `json:"message"`
	Tag     string `json:"tag"`
}

// Add accepts a variadic parameters of tags.
func (ai *AutomationItems) Add(tags ...string) ([]string, []ErrResult, error) {
	if ai == nil {
		return nil, nil, errors.New("AutomationItems is nil")
	}
	var nodes []string
	var errResult []ErrResult
	for _, tag := range tags {
		err := ai.addSingle(tag)
		if err != nil {
			errResult = append(errResult, ErrResult{Message: err.Error(), Tag: tag})
		} else {
			nodes = append(nodes, tag)
		}
	}

	return nodes, errResult, nil
}

// Remove removes the tag.
func (ai *AutomationItems) Remove(tag string) {
	if ai == nil {
		return
	}
	item, ok := ai.items[tag]
	if ok && item != nil {
		item.Release()
	}
	delete(ai.items, tag)
}

func ensureInt16(q interface{}) int16 {
	if v16, ok := q.(int16); ok {
		return v16
	}
	if v32, ok := q.(int32); ok && v32 >= -32768 && v32 < 32768 {
		return int16(v32)
	}
	return 0
}

// readFromOpc reads from the server and returns an Item and error.
func (ai *AutomationItems) readFromOpc(tag string, opcitem *ole.IDispatch) (Item, error) {
	if ai == nil {
		return Item{}, errors.New("AutomationItems is nil")
	}
	if opcitem == nil {
		return Item{}, fmt.Errorf("opcitem is nil for tag %s", tag)
	}

	v := ole.NewVariant(ole.VT_R4, 0)
	q := ole.NewVariant(ole.VT_INT, 0)
	ts := ole.NewVariant(ole.VT_DATE, 0)

	t := time.Now()

	_, err := oleutil.CallMethod(opcitem, "Read", OPCCache, &v, &q, &ts)
	opcReadsDuration.Observe(time.Since(t).Seconds())

	if err != nil {
		opcReadsCounter.WithLabelValues("failed").Inc()
		return Item{}, err
	}
	opcReadsCounter.WithLabelValues("success").Inc()

	val := v.Value()
	qVal := q.Value()
	tVal, _ := ts.Value().(time.Time)

	return Item{
		Tag:       tag,
		Value:     val,
		Quality:   ensureInt16(qVal),
		Timestamp: tVal,
	}, nil
}

// writeToOpc writes value to opc tag and return an error
func (ai *AutomationItems) writeToOpc(opcitem *ole.IDispatch, value interface{}) error {
	if ai == nil {
		return errors.New("AutomationItems is nil")
	}
	if opcitem == nil {
		return errors.New("opcitem is nil")
	}
	_, err := oleutil.CallMethod(opcitem, "Write", value)
	return err
}

// Close closes the OLE objects in AutomationItems.
func (ai *AutomationItems) Close() {
	if ai == nil {
		return
	}
	for key, opcitem := range ai.items {
		if opcitem != nil {
			opcitem.Release()
		}
		delete(ai.items, key)
	}
	if ai.addItemObject != nil {
		ai.addItemObject.Release()
		ai.addItemObject = nil
	}
}

// NewAutomationItems returns a new AutomationItems instance.
func NewAutomationItems(opcitems *ole.IDispatch) *AutomationItems {
	if opcitems == nil {
		logger.Println("NewAutomationItems received nil opcitems")
	}
	ai := AutomationItems{addItemObject: opcitems, items: make(map[string]*ole.IDispatch)}
	return &ai
}

type opcConnectionImpl struct {
	*AutomationObject
	*AutomationItems
	Server string
	Nodes  []string
	mu     sync.Mutex
}

// ReadItem returns an Item for a specific tag.
func (conn *opcConnectionImpl) ReadItem(tag string) Item {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.AutomationItems == nil || conn.AutomationItems.items == nil {
		logger.Printf("AutomationItems is nil, cannot read item %s", tag)
		return Item{}
	}

	opcitem, ok := conn.AutomationItems.items[tag]
	if !ok {
		logger.Printf("Tag %s not found. Add it first before reading it.", tag)
		return Item{}
	}

	item, err := conn.AutomationItems.readFromOpc(tag, opcitem)
	if err == nil {
		return item
	}
	logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
	conn.fix()
	return Item{}
}

// Write writes a value to the OPC Server.
func (conn *opcConnectionImpl) Write(tag string, value interface{}) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.AutomationItems == nil || conn.AutomationItems.items == nil {
		return errors.New("AutomationItems is nil, cannot write")
	}

	opcitem, ok := conn.AutomationItems.items[tag]
	if !ok {
		logger.Printf("Tag %s not found. Add it first before writing to it.", tag)
		return errors.New("no Write performed")
	}
	return conn.AutomationItems.writeToOpc(opcitem, value)
}

// Read returns a map of the values of all added tags.
func (conn *opcConnectionImpl) Read() (map[string]Item, error) {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	allTags := make(map[string]Item)
	if conn.AutomationItems == nil || conn.AutomationItems.items == nil {
		return nil, errors.New("AutomationItems is nil")
	}

	for tag, opcitem := range conn.AutomationItems.items {
		if opcitem == nil {
			allTags[tag] = Item{Tag: tag, Err: "opcitem is nil"}
			continue
		}
		item, err := conn.AutomationItems.readFromOpc(tag, opcitem)
		if err != nil {
			logger.Printf("Cannot read %s: %s. Trying to fix.", tag, err)
			allTags[tag] = Item{Tag: tag, Err: err.Error()}
			continue
		}
		allTags[tag] = item
	}
	return allTags, nil
}

// Tags returns the currently active tags
func (conn *opcConnectionImpl) Tags() []string {
	if conn.AutomationItems == nil || conn.AutomationItems.items == nil {
		return []string{}
	}
	var tags []string
	for tag := range conn.AutomationItems.items {
		tags = append(tags, tag)
	}
	return tags
}

// fix tries to reconnect if connection is lost by creating a new connection
// with AutomationObject and recreating AutomationItems.
func (conn *opcConnectionImpl) fix() error {
	var err error
	if !conn.IsConnected() {
		retries := 0
		for {
			tags := conn.Tags()
			if conn.AutomationItems != nil {
				conn.AutomationItems.Close()
			}
			conn.AutomationItems, err = conn.TryConnect(conn.Server, conn.Nodes)
			if retries >= 10 {
				logger.Printf("Failed to reconnect after %d retries. Giving up.", retries)
				return err
			}
			if err != nil {
				retries += 1
				logger.Println(err)
				time.Sleep(10 * time.Second)
				continue
			}
			if conn.AutomationItems != nil {
				nodesAdded, errors, err := conn.Add(tags...)

				if len(nodesAdded) > 0 {
					logger.Printf("Added %d tags", len(tags))
				}
				if len(errors) > 0 {
					logger.Printf("Failed to this tags: %v", errors)
				}
				if err != nil {
					logger.Printf("Failed to add tags: %s", err)
					return err
				}
			}
			break
		}
	}
	return nil
}

// Close closes the embedded types.
func (conn *opcConnectionImpl) Close() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.AutomationItems != nil {
		conn.AutomationItems.Close()
		conn.AutomationItems = nil
	}
	if conn.AutomationObject != nil {
		conn.AutomationObject.Close()
		conn.AutomationObject = nil
	}
}

// NewConnection establishes a connection to the OpcServer object.
func NewConnection(server string, nodes []string, tags []string) (Connection, []ErrResult, error) {
	object := NewAutomationObject()
	if object == nil || object.object == nil {
		return &opcConnectionImpl{}, nil, errors.New("could not create automation object")
	}

	items, err := object.TryConnect(server, nodes)
	if err != nil {
		object.Close()
		return &opcConnectionImpl{}, nil, err
	}
	nodes, nodesErrors, err := items.Add(tags...)
	if err != nil {
		items.Close()
		object.Close()
		return &opcConnectionImpl{}, nodesErrors, err
	}
	conn := opcConnectionImpl{
		AutomationObject: object,
		AutomationItems:  items,
		Server:           server,
		Nodes:            nodes,
	}

	return &conn, nodesErrors, nil
}

// CreateBrowser creates an opc browser representation
func CreateBrowser(server string, nodes []string, browser string) (*Tree, error) {
	object := NewAutomationObject()
	if object == nil || object.object == nil {
		return nil, errors.New("could not create automation object for browser")
	}
	defer object.Close()
	_, err := object.TryConnect(server, nodes)
	if err != nil {
		return nil, err
	}
	return object.CreateBrowser(browser)
}
