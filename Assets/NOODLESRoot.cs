#nullable enable

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using PeterO.Cbor;
using UnityEngine;
using UnityEngine.Assertions;
using UnityEngine.Rendering;

/// <summary>
/// Enumeration of all NOODLES component types
/// </summary>
public enum ComponentType {
    Method,
    Signal,
    Entity,
    Plot,
    Buffer,
    BufferView,
    Material,
    Image,
    Texture,
    Sampler,
    Light,
    Geometry,
    Table,

    NONE,
}

/// <summary>
/// Enumeration of message actions on components
/// </summary>
enum ComponentAction {
    Create,
    Delete,
    Update,

    NONE,
}

public delegate void MessageReplyDelegate(CBORObject reply);
public delegate void EntityChangeDelegate(GameObject obj, CBORObject content);

/// <summary>
/// Root NOODLES script. Attach this script to an object, and it will connect to the given host on start, and then populate the scene
/// </summary>
public class NOODLESRoot : MonoBehaviour
{
    /// <summary>
    /// Server to connect to
    /// </summary>
    public string serverURI = "ws://localhost:50000";

    /// <summary>
    /// Websocket channel to the server
    /// </summary>
    readonly ClientWebSocket ws;

    /// <summary>
    /// Task that continually reads from the websocket
    /// </summary>
    Task? read_task;

    /// <summary>
    /// Task that continually writes messages to the websocket
    /// </summary>
    Task? write_task;

    /// <summary>
    /// If a data URL is encountered in the incoming stream, the read task will fetch those URLs and store them here
    /// </summary>
    readonly ConcurrentDictionary<string, ReadOnlyMemory<byte>> fetched_urls;

    /// <summary>
    /// A queue of incoming messages, placed by the read task
    /// </summary>
    readonly ConcurrentQueue<CBORObject> incoming_messages;

    /// <summary>
    /// A queue of outgoing messages, placed by the main thread
    /// </summary>
    ConcurrentQueue<CBORObject> outgoing_messages;

    /// <summary>
    /// Remote data URLs are cached here for re-use
    /// </summary>
    readonly BufferCache Cache;

    /// <summary>
    /// Storage for NOODLES component state
    /// </summary>
    readonly ComponentPack components_pack;

    /// <summary>
    /// Storage for message response callbacks
    /// </summary>
    Dictionary<Guid, MessageReplyDelegate> message_response;

    /// <summary>
    /// User selected shader to apply to new objects
    /// </summary>
    public Shader? newMaterialShader;


    public event EntityChangeDelegate? OnEntityCreated;
    public event EntityChangeDelegate? OnEntityUpdated;

    public NOODLESRoot() {
        ws = new();
        fetched_urls = new(2, 24);
        incoming_messages = new();
        outgoing_messages = new();
        Cache = new();
        components_pack = new();
        message_response = new();
    }

    public void FireOnEntityCreated(GameObject obj, CBORObject content) {
        OnEntityCreated?.Invoke(obj, content);
    }

    public void FireOnEntityUpdated(GameObject obj, CBORObject content) {
        OnEntityUpdated?.Invoke(obj, content);
    }

    /// <summary>
    /// Fetch the NOODLES component with given type and ID
    /// </summary>
    /// <param name="type">Type of component to fetch</param>
    /// <param name="id">ID of that component</param>
    /// <returns>a component if it exists, null otherwise</returns>
    public INoodlesComponent? GetNoodlesComponent(ComponentType type, NooID id) {
        return components_pack.GetNoodlesComponent(type, id);
    }

    /// <summary>
    /// On script initialization, start connecting to the given server
    /// </summary>
    async void Start()
    {
        Debug.Log("Starting up connection to " + serverURI);
        await ws.ConnectAsync(new System.Uri(serverURI), default);

        // send opening message
        SendMessage(new Introduction("Unity Client"));

        // start socket read task
        read_task = new Task(() => ReadMessagesTask());
        read_task.Start();

        write_task = new Task(() => WriteMessagesTask());
        write_task.Start();

        Debug.Log("Startup complete");
    }
    /// <summary>
    /// Dispatch a message to the server. This should be called from the main thread.
    /// </summary>
    /// <typeparam name="T">Type of the message</typeparam>
    /// <param name="message">Message to send</param>
    /// <returns>async task</returns>
    void SendMessage<T>(T message) where T : IMessage{
        Debug.Log("Sending message");

        // tuples are primitive here so we have to do this the bad way
        outgoing_messages.Enqueue(CBORObject.NewArray().Add(message.MessageId()).Add(message.ToCBOR()));
    }

    /// <summary>
    /// An async task to continually read from the websocket and pre-process incoming messages
    /// </summary>
    async void ReadMessagesTask() {
        Debug.Log("Starting read message task");

        while (true) {
            var message_array = await ReadNextMessage();

            incoming_messages.Enqueue(message_array);
        }
    }

    async void WriteMessagesTask() {
        while (true) {
            if (outgoing_messages.TryDequeue(out CBORObject message)) {
                var bytes = message.EncodeToBytes();
                await ws.SendAsync(bytes, WebSocketMessageType.Binary, true, CancellationToken.None);
            } else {
                // cmon now. surely we can do better
                await Task.Delay(16);
            }
        }
    }

    /// <summary>
    /// Consume a message array from the server and handle each message. This must be run from the main thread.
    /// </summary>
    /// <param name="arr">CBOR message array to handle</param>
    void ParseMessageArray(CBORObject arr) {
        Debug.Log("Parse message array: " + arr.Count.ToString());
        if (arr.Count % 2 != 0) {
            Debug.LogError("Malformed message array");
            return;
        }

        int cursor = 0;

        while (cursor < arr.Count) {
            //first get id
            var id = arr[cursor].AsInt32();
            // content is next
            var content = arr[cursor+1];

            Debug.Log(string.Format("Message: {0} => {1}", id, content));

            try
            {

                if (id <= 30){
                    components_pack.Handle(this, id, content);
                } else {
                    HandleNonComponentMessage(id, content);
                }
            }
            catch (System.Exception e)
            {
                Debug.LogException(e);
                Debug.LogError("Error handling " + id + " content " + content.ToString());
                throw;
            }
           
            cursor += 2;
        }
    }

    void HandleNonComponentMessage(int message_id, CBORObject content) {
        switch (message_id) {
            case 31:
                // document update
                return;
            case 32:
                // document reset
                return;
            case 33:
                // signal invoke
                return;
            case 34:
                // method reply
                var uuid = Guid.Parse(content["invoke_id"].AsString());
                if (!message_response.TryGetValue(uuid, out MessageReplyDelegate to_call)) {
                    Debug.LogWarning("Server sending response to message we did not send.");
                    return;
                }

                to_call?.Invoke(content);

                return;
            case 35:
                // document ready
                return;
        }
    }

    /// <summary>
    /// Obtain a new message array from the websocket
    /// </summary>
    /// <returns>a new message array, empty if the socket is unreadable</returns>
    async Task<CBORObject> ReadNextMessage() {
        // we started out with a nice stream system that completely failed. doing this the hard way

        // if we are reading from a dead socket, just return
        if (ws.State == WebSocketState.Closed || ws.State == WebSocketState.CloseSent || ws.State == WebSocketState.CloseReceived ) {
            return CBORObject.Undefined;
        }

        // buffer storage for message chunks
        var segment = new byte[1024];

        // appendable stream for the whole message
        var mem_stream = new MemoryStream();

        while (true){
            // read from socket to chunk buffer
            WebSocketReceiveResult result = await ws.ReceiveAsync(segment, CancellationToken.None);

            // socket could have been closed at this time
            if (result.MessageType == WebSocketMessageType.Close) {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "Connection dropped", CancellationToken.None);
                return CBORObject.Undefined;
            }

            // if no bytes recv, probably no more bytes left, so break
            if (result.Count == 0) {
                break;
            }

            // write recv'ed bytes to whole stream
            mem_stream.Write(segment, 0, result.Count);

            // if this is not the end of the message, keep waiting for more
            if (result.EndOfMessage) {
                break;
            }
        }

        // reset stream to start for CBOR parsing
        mem_stream.Seek(0, SeekOrigin.Begin);

        // decode message
        var message = CBORObject.DecodeFromBytes(mem_stream.ToArray());

        // by the spec, all messages are arrays
        if (message.Type != CBORType.Array) {
            Debug.Log("Bad message");
            return CBORObject.Undefined;
        }

        // scan messages for buffer downloads so we can start pulling them down
        int cursor = 0;

        while (cursor < message.Count) {
            var id = message[cursor].AsInt32();
            var content = message[cursor+1];

            // should be BufferCreate and ImageCreate that can point to remote resources
            switch (id) {
                case 10:
                    NooTools.ActionOnContent("uri_bytes", content, (CBORObject value) => {
                        string uri_target = value.AsString();
                        var client = new System.Net.Http.HttpClient();

                        var request = Task.Run( () => client.GetByteArrayAsync(uri_target) ).Result;

                        fetched_urls[uri_target] = new ReadOnlyMemory<byte>(request);
                    });
                    break;
                case 17:
                    NooTools.ActionOnContent("uri_source", content, (CBORObject value) => {
                        string uri_target = value.AsString();
                        var client = new System.Net.Http.HttpClient();

                        var request = Task.Run( () => client.GetByteArrayAsync(uri_target) ).Result;

                        fetched_urls[uri_target] = new ReadOnlyMemory<byte>(request);
                    });
                    break;
            }
           
            cursor += 2;
        }

        return message;
    }

    /// <summary>
    /// Spin off any messages in the incoming queue
    /// </summary>
    void ConsumeMessages() {
        while (incoming_messages.TryDequeue(out CBORObject message_pack))
        {
            ParseMessageArray(message_pack);
        }
    }

    /// <summary>
    /// Obtain an associated buffer of a URI
    /// </summary>
    /// <param name="uri">URI to look for</param>
    /// <returns>Buffer for given URI</returns>
    /// <exception cref="NullReferenceException">if URI has not been fetched</exception>
    public ReadOnlyMemory<byte> BufferCacheGet(string uri) {
        if (Cache.Has(uri)) {
            return Cache.Fetch(uri);
        }

        if (!fetched_urls.TryRemove(uri, out ReadOnlyMemory<byte> data)) {
            Debug.Log(string.Format("Unable to find buffer: {0}", uri));
            throw new NullReferenceException("Missing buffer");
        }

        Cache.Install(uri, data);

        return data;
    }

    public void BufferCacheRelease(string uri) {
        Cache.Replace(uri);
    }
   
    void Update()
    {
        ConsumeMessages();
    }

    public void invoke_method(NooID methodid, NooID entityid, List<CBORObject> args, MessageReplyDelegate del) {
        // omit check to see if method is on the entity. hopefully we just dont do that.

        var nid = Guid.NewGuid();

        message_response[nid] = del;

        SendMessage(new InvokeMethodMessage(nid, methodid, entityid, args));

    }
}

/// <summary>
/// Cache fetched buffers
/// </summary>
public class BufferCache {
    // stupid. how can we use the ref counting nicer?

    /// <summary>
    /// Represents a reference counted byte array
    /// </summary>
    class CacheLine {
        public int rc = 0;
        public ReadOnlyMemory<byte> data;

        public CacheLine(ReadOnlyMemory<byte> bytes) {
            data = bytes;
        }
    }

    /// <summary>
    /// URLs to cached bytes
    /// </summary>
    readonly Dictionary<string, CacheLine> lines;

    public BufferCache() {
        lines = new Dictionary<string, CacheLine>();
    }

    public void Install(string uri, ReadOnlyMemory<byte> data) {
        lines.Add(uri, new CacheLine(data));
    }

    public bool Has(string uri) {
        return lines.ContainsKey(uri);
    }

    public ReadOnlyMemory<byte> Fetch(string uri) {
        var line = lines[uri];
        line.rc += 1;
        return line.data;
    }

    public void Replace(string uri) {
        var line = lines[uri];
        line.rc -= 1;
        if (line.rc <= 0) {
            lines.Remove(uri);
        }
    }
}


#region Noodles ID Type

/// <summary>
/// A NOODLES ID. This consists of a slot and a generation number
/// </summary>
public struct NooID: IEquatable<NooID> {
    public uint slot;
    public uint gen;

    /// <summary>
    /// Parse an ID from CBOR
    /// </summary>
    /// <param name="value">Object to parse from. Should be a CBOR Array</param>
    /// <returns>New ID</returns>
    public static NooID FromCBOR(CBORObject value) {
        return new NooID {
            slot = value[0].ToObject<uint>(),
            gen = value[1].ToObject<uint>(),
        };
    }

    public readonly CBORObject ToCBOR() {
        return CBORObject.NewArray().Add(slot).Add(gen);
    }

    public override readonly bool Equals(object other)
    {
        if (other is not NooID) return false;
        NooID o = (NooID) other;
        return slot == o.slot && gen == o.gen;
    }

    public readonly bool Equals(NooID other)
    {
        return slot == other.slot && gen == other.gen;
    }

    public static bool operator == (NooID a, NooID b) {
        return a.slot == b.slot && a.gen == b.gen;
    }

    public static bool operator != (NooID a, NooID b) {
        return !(a.slot == b.slot && a.gen == b.gen);
    }

    public readonly bool IsNull() {
        const uint NULL = uint.MaxValue;
        return slot == NULL || gen == NULL;
    }

    public override readonly int GetHashCode()
    {
        return HashCode.Combine(slot, gen);
    }
}

#endregion

#region Messages

/// <summary>
/// Introduction message. Sends the client name.
/// </summary>
class Introduction : IMessage {
    public string client_name;

    public Introduction(string name) {
        client_name = name;
    }

    public ushort MessageId()
    {
        return 0;
    }

    public CBORObject ToCBOR() {
        return CBORObject
            .NewMap()
            .Add("client_name", client_name);
    }
}

class InvokeMethodMessage : IMessage {
    Guid invoke_id;
    public NooID method_id;
    public NooID entity_id;
    public List<CBORObject> args;

    public InvokeMethodMessage(Guid iid, NooID mid, NooID eid, List<CBORObject> arguments) {
        invoke_id = iid;
        method_id = mid;
        entity_id = eid;
        args = arguments;
    }

    public ushort MessageId()
    {
        return 1;
    }

    public CBORObject ToCBOR() {
        return CBORObject
            .NewMap()
            .Add("invoke_id", invoke_id.ToString())
            .Add("method_id", method_id.ToCBOR())
            .Add("context", CBORObject.NewMap()
                                .Add("entity", entity_id.ToCBOR())
                )
            .Add("args", args);
    }
}

/// <summary>
/// Message interface for client-side messages
/// </summary>
interface IMessage {
    ushort MessageId();
    CBORObject ToCBOR();
}

#endregion

/// <summary>
/// Interface for NOODLES component handlers
/// </summary>
public interface INoodlesComponent {
    abstract void OnCreate(NOODLESRoot root, CBORObject content);
    abstract void OnDelete(NOODLESRoot root);
    virtual void OnUpdate(NOODLESRoot root, CBORObject content) {}
}

public class BlankComponent : INoodlesComponent {
    public void OnCreate(NOODLESRoot root, CBORObject content) {
        Debug.Log("Creating component: ");
    }
    public void OnDelete(NOODLESRoot root) {
        Debug.Log("Deleting component: ");
    }

    public void OnUpdate(NOODLESRoot root, CBORObject content) {
        Debug.Log("Updating component: ");
    }
}

/// <summary>
/// Models a used component slot in a ComponentList
/// </summary>
struct ComponentSlot {
    public NooID id;
    public INoodlesComponent component;

    public ComponentSlot(NooID place, INoodlesComponent comp) {
        id = place;
        component = comp;
    }
}

/// <summary>
/// Container for NOODLES components of a given type
/// </summary>
public class ComponentList {
    readonly Dictionary<NooID, INoodlesComponent> component_collection;  

    public ComponentList() {
        component_collection = new Dictionary<NooID, INoodlesComponent>();
    }

    /// <summary>
    /// Obtain a component by ID
    /// </summary>
    /// <param name="id">ID of component</param>
    /// <returns>Requested component, or null if ID was not found</returns>
    public INoodlesComponent? Get(NooID id) {
        if (component_collection.TryGetValue(id, out INoodlesComponent ret))
        {
            return ret;
        }
        return null;
    }

    public void Insert(NOODLESRoot root, NooID place, INoodlesComponent comp, CBORObject content) {
        component_collection.Add(place, comp);
        comp.OnCreate(root, content);
    }

    public void Update(NOODLESRoot root, NooID place, CBORObject content) {
        component_collection[place].OnUpdate(root, content);
    }

    public void Remove(NOODLESRoot root, NooID place) {
        component_collection[place].OnDelete(root);
        component_collection.Remove(place);
    }
}


/// <summary>
/// Collects all NOODLES component lists
/// </summary>
public class ComponentPack {
    /// <summary>
    /// Max number of components in the current spec
    /// </summary>
    private const int NUM_COMPONENTS = 14;
    readonly List<ComponentList> components;

    public ComponentPack() {
        // build component lists for everything
        components = new List<ComponentList>();

        for (int i = 0; i < NUM_COMPONENTS; i++){
            components.Add( new ComponentList() );
        }
    }

    /// <summary>
    /// Ask for a NOODLES component by ID and Type
    /// </summary>
    /// <param name="type">Type of the component you are searching for</param>
    /// <param name="id">ID of the component</param>
    /// <returns>Component, if found. Null otherwise.</returns>
    public INoodlesComponent? GetNoodlesComponent(ComponentType type, NooID id) {
        return components[(int) type].Get(id);
    }

    /// <summary>
    /// Handle an incoming message
    /// </summary>
    /// <param name="root">Parent unity entity, needed to install child nodes</param>
    /// <param name="message_id">ID of the message from the spec</param>
    /// <param name="content">Message content</param>
    public void Handle(NOODLESRoot root, int message_id, CBORObject content) {

        switch (message_id) {
            case 31: return;
            case 32: return;
            case 33: return;
            case 34: return;
            case 35: return;
        }

        // Dispatch message to proper component list

        // Obtain a context id from the message content, i.e. what this message acts on
        var id = NooTools.IDFromContent(content);

        //Debug.Log("On message: " + id.ToString());

        if (id.IsNull()) {
            Debug.Log("ID is null!");
            return;
        }

        var component = NooTools.TypeFromMessage(message_id);
        var action = NooTools.ActionFromMessage(message_id);

        if (component == ComponentType.NONE || action == ComponentAction.NONE) {
            return;
        }

        var clist = components[(int)component];

        switch (action) {
            case ComponentAction.Create: 
                var new_component = NooTools.MakeNewComponent(component);
                clist.Insert(root, id, new_component, content);
                break;
            case ComponentAction.Update: 
                clist.Update(root, id, content);
                break;
            case ComponentAction.Delete: 
                clist.Remove(root, id);
                break;
        }

        return;
    }
}

static class NooTools {

    public static INoodlesComponent MakeNewComponent(ComponentType type) {
        return type switch {
            ComponentType.Buffer => new BufferComponent(),
            ComponentType.BufferView => new BufferViewComponent(),
            ComponentType.Image => new ImageComponent(),
            ComponentType.Texture => new TextureComponent(),
            ComponentType.Material => new MaterialComponent(),
            ComponentType.Geometry => new GeometryComponent(),
            ComponentType.Entity => new EntityComponent(),
            _ => new BlankComponent(),
        };
    }

    public static NooID IDFromContent(in CBORObject content) {
        return NooID.FromCBOR(content["id"]);
    }

    public static string name_from_content(in CBORObject content, in string def) {
        if (content.ContainsKey("name")) {
            return content["name"].ToString();
        }

        var id = IDFromContent(content);
        
        return string.Format("{0} {1}/{2}", def, id.slot, id.gen);
    }

    public static CBORObject? ValueFromContent(in string key, in CBORObject content) {
        if (content.ContainsKey(key)) {
            return content[key];
        }
        
        return null;
    }

    public static T TypedFromContent<T>(in string key, in CBORObject content, T def) {
        if (content.ContainsKey(key)) {
            return content[key].ToObject<T>();
        }
        
        return def;
    }

    public static void ActionOnContent(in string key, in CBORObject content, Action<CBORObject> action) {
        var c = ValueFromContent(key, content);

        if (c is not null) {
            action.Invoke(c!);
        }
    }

    public static void EitherOnContent(in string key, in CBORObject content, Action<CBORObject> action, Action false_action) {
        var c = ValueFromContent(key, content);

        if (c is not null) {
            action.Invoke(c!);
        } else {
            false_action.Invoke();
        }
    }

    public static Color ArrayToColor(CBORObject arr) {
        var c = new Color(1,1,1,1);

        for (int i = 0; i < Math.Min(4, arr.Count); i++) {
            c[i] = arr[i].AsSingle();
        }

        return c;
    }


    public static ComponentType TypeFromMessage(int message_id) {
        return message_id switch
        {
            0 => ComponentType.Method,
            1 => ComponentType.Method,
            2 => ComponentType.Signal,
            3 => ComponentType.Signal,
            4 => ComponentType.Entity,
            5 => ComponentType.Entity,
            6 => ComponentType.Entity,
            7 => ComponentType.Plot,
            8 => ComponentType.Plot,
            9 => ComponentType.Plot,
            10 => ComponentType.Buffer,
            11 => ComponentType.Buffer,
            12 => ComponentType.BufferView,
            13 => ComponentType.BufferView,
            14 => ComponentType.Material,
            15 => ComponentType.Material,
            16 => ComponentType.Material,
            17 => ComponentType.Image,
            18 => ComponentType.Image,
            19 => ComponentType.Texture,
            20 => ComponentType.Texture,
            21 => ComponentType.Sampler,
            22 => ComponentType.Sampler,
            23 => ComponentType.Light,
            24 => ComponentType.Light,
            25 => ComponentType.Light,
            26 => ComponentType.Geometry,
            27 => ComponentType.Geometry,
            28 => ComponentType.Table,
            29 => ComponentType.Table,
            30 => ComponentType.Table,
            _ => ComponentType.NONE,
        };
    }

    public static ComponentAction ActionFromMessage(int message_id) {
        return message_id switch
        {
            0 => ComponentAction.Create,
            1 => ComponentAction.Delete,
            2 => ComponentAction.Create,
            3 => ComponentAction.Delete,
            4 => ComponentAction.Create,
            5 => ComponentAction.Update,
            6 => ComponentAction.Delete,
            7 => ComponentAction.Create,
            8 => ComponentAction.Update,
            9 => ComponentAction.Delete,
            10 => ComponentAction.Create,
            11 => ComponentAction.Delete,
            12 => ComponentAction.Create,
            13 => ComponentAction.Delete,
            14 => ComponentAction.Create,
            15 => ComponentAction.Update,
            16 => ComponentAction.Delete,
            17 => ComponentAction.Create,
            18 => ComponentAction.Delete,
            19 => ComponentAction.Create,
            20 => ComponentAction.Delete,
            21 => ComponentAction.Create,
            22 => ComponentAction.Delete,
            23 => ComponentAction.Create,
            24 => ComponentAction.Update,
            25 => ComponentAction.Delete,
            26 => ComponentAction.Create,
            27 => ComponentAction.Update,
            28 => ComponentAction.Create,
            29 => ComponentAction.Update,
            30 => ComponentAction.Delete,
            _ => ComponentAction.NONE,
        };
    }
}


#region Buffer

class BufferComponent : INoodlesComponent
{
    ReadOnlyMemory<byte>? storage;
    string fetch_url;

    public BufferComponent() {
        fetch_url = "";
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new buffer: " +  NooTools.name_from_content(content, "Buffer"));

        NooTools.ActionOnContent("inline_bytes", content, (CBORObject value) => {
            storage = new ReadOnlyMemory<byte>(value.ToObject<byte[]>());
        });

        NooTools.ActionOnContent("uri_bytes", content, (CBORObject value) => {
            fetch_url = value.AsString();
            
            storage = root.BufferCacheGet(fetch_url);
        });
    }

    public void OnDelete(NOODLESRoot root)
    {
        Debug.Log("Destroying buffer");
        if (fetch_url.Length > 0){
            root.BufferCacheRelease(fetch_url);
        }
    }

    public ReadOnlyMemory<byte> GetBytes() {
        return (ReadOnlyMemory<byte>)storage!;
    }
}

#endregion

#region BufferView
public class BufferViewComponent : INoodlesComponent
{
    BufferComponent? buffer;

    long offset;
    long length;

    ReadOnlyMemory<byte>? storage;

    public BufferViewComponent() {
        offset = 0;
        length = 0;
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new buffer view: " +  NooTools.name_from_content(content, "BufferView"));

        buffer = (BufferComponent?)root.GetNoodlesComponent(ComponentType.Buffer, NooID.FromCBOR(content["source_buffer"]));

        offset = content["offset"].ToObject<long>();
        length = content["length"].ToObject<long>();

        storage ??= buffer!.GetBytes().Slice((int)offset, (int)length);

        if (storage == null) {
            Debug.LogError("Source buffer is null!");
        }

        if (offset > int.MaxValue || length > int.MaxValue) {
            Debug.LogWarning("Offsets are too large, and will cause problems!");
        }
    }

    public void OnDelete(NOODLESRoot root)
    {
        Debug.Log("Destroying bufferview");
    }

    public ReadOnlyMemory<byte> GetBytes() {
        Assert.IsTrue(storage != null);
        return storage ?? new ReadOnlyMemory<byte>(); 
    }
}

#endregion

#region Geometry

public struct GeometryAttrib
{
    public NooID buffer_view;
    public string semantic;
    public int offset;
    public int stride;
    public string format;
    public bool normalized;
}

// Unity has no concept of dynamic vertex layouts. Thus, we have this garbage

namespace VertexFormats {
public struct Vertex8 {
    public int _4, _8;
}

public struct Vertex12 {
    public int _4, _8, _12;
}

public struct Vertex16 {
    public int _4, _8, _12, _16;
}

public struct Vertex20 {
    public int _4, _8, _12, _16, _20;
}

public struct Vertex24 {
    public int _4, _8, _12, _16, _20, _24;
}

public struct Vertex28 {
    public int _4, _8, _12, _16, _20 ,_24, _28;
}

public struct Vertex32 {
    public int _4, _8, _12, _16, _20, _24, _28, _32;
}
}

public class GeometryComponent : INoodlesComponent
{
    // Everything here SUCKS

    Mesh[] meshes;
    Material[] materials;

    public GeometryComponent() {
        meshes = new Mesh[0];
        materials = new Material[0];
    }

    public Mesh[] MeshList() {
        return meshes;
    }

    public Material[] MaterialList() {
        return materials;
    }

    public static VertexAttribute SemanticToAttribute(string semantic) {
        switch (semantic)
        {
            case "POSITION":
                return VertexAttribute.Position;
            case "NORMAL":
                return VertexAttribute.Normal;
            case "TANGENT":
                return VertexAttribute.Tangent;
            case "TEXTURE":
                return VertexAttribute.TexCoord0;
            case "COLOR":
                return VertexAttribute.Color;
            default:
                Debug.LogError(string.Format("Unknown attribute {0}", semantic));
                return VertexAttribute.Position;
        }
    }

    public static (VertexAttributeFormat,int) FormatToFormat(string format) {
        switch (format)
        {
            case "U8":
                return (VertexAttributeFormat.UInt8, 1);
            case "U16":
                return (VertexAttributeFormat.UInt16, 1);
            case "U32":
                return (VertexAttributeFormat.UInt32, 1);
            case "U8VEC4":
                return (VertexAttributeFormat.UInt8, 4);
            case "U16VEC2":
                return (VertexAttributeFormat.UInt16, 2);
            case "VEC2":
                return (VertexAttributeFormat.Float32, 2);
            case "VEC3":
                return (VertexAttributeFormat.Float32, 3);
            case "VEC4":
                return (VertexAttributeFormat.Float32, 4);
            default:
                Debug.LogError(string.Format("Unknown format {0}", format));
                return (VertexAttributeFormat.UInt8, 1);
        }
    }

    public static int FormatToStride(string format) {
        switch (format)
        {
            case "U8":
                return 1;
            case "U16":
                return 2;
            case "U32":
                return 4;
            case "U8VEC4":
                return 4;
            case "U16VEC2":
                return 4;
            case "VEC2":
                return 8;
            case "VEC3":
                return 12;
            case "VEC4":
                return 16;
            default:
                Debug.LogError(string.Format("Unknown format {0}", format));
                return 1;
        }
    }

    static T[] ConvertByteArrayToFakeStructs<T>(byte[] source, int vertex_size, int vertex_count) {
        var ret = new T[vertex_count];
        IntPtr ptr = Marshal.AllocHGlobal(vertex_size);
        for (int i = 0; i < vertex_count; i++) {
            Marshal.Copy(source, i * vertex_size, ptr, vertex_size);
            ret[i] = (T) Marshal.PtrToStructure(ptr, typeof(T));
            
        }
        Marshal.FreeHGlobal(ptr);
        return ret;
    }

    static void SetMeshData(Mesh mesh, byte[] source, int vertex_size, int vertex_count, int stream) {
        switch (vertex_size) {
            case 8: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex8>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 12: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex12>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 16: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex16>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 20: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex20>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 24: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex24>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 28: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex28>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
            case 32: 
                mesh.SetVertexBufferData(ConvertByteArrayToFakeStructs<VertexFormats.Vertex32>(source, vertex_size, vertex_count), 0, 0, vertex_count, stream); 
                break;
        }
    }
    

    /// <summary>
    /// Ask if this stream map is interleaved. If so, we can quickly load vertex data. If not, we will need to go with the slower API
    /// </summary>
    /// <param name="stream_map"></param>
    /// <returns></returns>
    public bool IsInterleaved(Dictionary<int, List<GeometryAttrib>> stream_map) {
        foreach (var stream_pack in stream_map) {
            stream_pack.Value.Sort((a, b) => a.offset.CompareTo(b.offset));

            int pointer = 0;
            int running_stride = -1;

            foreach (var att in stream_pack.Value) {
                // we want each att to be packed to a previous attr.
                // and the strides should be the vertex stride.

                var u_format = FormatToFormat(att.format);

                if (running_stride < 0) {
                    running_stride = att.stride;
                } else {
                    // to make sure they are lined up, all att strides should be the same
                    if (running_stride != att.stride) {
                        return false;
                    }
                }

                if (pointer != att.offset) {
                    return false;
                }

                pointer += FormatToStride(att.format);
            }
            
        }

        return true;
    }

    public bool InstallPatch(int index, NOODLESRoot root, CBORObject patch)
    {
        Mesh new_mesh = new();
        meshes[index] = new_mesh;

        // well there are a lot of ways of doing this

        // since i am crunched on time, we are going with the bad way

        var attributes = patch["attributes"];
        var vertex_count = patch["vertex_count"].ToObject<int>();
        var type = patch["type"].AsString();

        if (type != "TRIANGLES")
        {
            // we dont handle that for now, but we also dont signal this as an error
            return true;
        }

        // split up mesh info based on which buffer or 'stream' the vert info is coming from
        var stream_map = new Dictionary<NooID, int>();
        var attrib_stream_map = new Dictionary<int, List<GeometryAttrib>>();
        var stream_stride = new Dictionary<int, int>();

        foreach (var attrib_obj in attributes.Values)
        {
            //Debug.Log("ATTRIB " + attrib_obj["offset"].Type.ToString());
            //Debug.Log("ATTRIB " + attrib_obj["offset"].AsInt32());
            var view = NooID.FromCBOR(attrib_obj["view"]);
            var semantic = attrib_obj["semantic"].AsString();
            var offset = NooTools.TypedFromContent("offset", attrib_obj, 0);
            var stride = NooTools.TypedFromContent("stride", attrib_obj, 0);
            var format = NooTools.TypedFromContent("format", attrib_obj, "");
            var normalized = NooTools.TypedFromContent("normalized", attrib_obj, false);

            if (stride < FormatToStride(format))
            {
                stride = FormatToStride(format);
            }

            if (!stream_map.ContainsKey(view))
            {
                stream_map[view] = stream_map.Count;
            }

            var stream = stream_map[view];
            stream_stride[stream] = stride;

            attrib_stream_map.TryAdd(stream, new List<GeometryAttrib>());

            attrib_stream_map[stream].Add(new GeometryAttrib
            {
                buffer_view = view,
                semantic = semantic,
                offset = offset,
                stride = stride,
                format = format,
                normalized = normalized,
            });
        }

        if (IsInterleaved(attrib_stream_map))
        {   
            AddVertexDataInterleaved(root, new_mesh, attributes, vertex_count, attrib_stream_map, stream_map, stream_stride);
        } else {
            AddVertexDataSlow(root, new_mesh, attributes, vertex_count, attrib_stream_map);
        }

        // and now index information

        NooTools.ActionOnContent("indices", patch, (CBORObject index_info) =>
        {
            Debug.Log("Adding index: " + index_info.ToString());

            BufferViewComponent buffer_view = (BufferViewComponent)root.GetNoodlesComponent(ComponentType.BufferView, NooID.FromCBOR(index_info["view"]))!;
            var count = index_info["count"].AsInt32();
            var offset = NooTools.TypedFromContent("offset", index_info, 0);
            var stride = NooTools.TypedFromContent("stride", index_info, 0);
            var format = index_info["format"].AsString();

            var bytes = buffer_view.GetBytes()[offset..];

            new_mesh.indexFormat = IndexFormat.UInt16;

            switch (format)
            {
                case "U8":
                    throw new NotImplementedException();
                case "U16":
                    bytes = bytes[..(2 * count)];
                    new_mesh.indexFormat = IndexFormat.UInt16;
                    new_mesh.SetIndices(MemoryMarshal.Cast<byte, ushort>(bytes.Span).ToArray(), MeshTopology.Triangles, 0);
                    break;
                case "U32":
                    bytes = bytes[..(4 * count)];
                    new_mesh.indexFormat = IndexFormat.UInt32;
                    new_mesh.SetIndices(MemoryMarshal.Cast<byte, int>(bytes.Span).ToArray(), MeshTopology.Triangles, 0);
                    break;
            }

        });

        // Finalize
        new_mesh.RecalculateBounds();
        Debug.Log("Bounding volume: " + new_mesh.bounds);

        //new_mesh.UploadMeshData(true);

        // adding material
        var material_comp = (MaterialComponent)root.GetNoodlesComponent(ComponentType.Material, NooID.FromCBOR(patch["material"]))!;

        materials[index] = material_comp.Material();

        return true;
    }

    private static void MakeNormalizedU16VEC2(byte[] bytes, int offset, ref Vector2 t) {
        Span<byte> span = bytes;
        t.x = BitConverter.ToUInt16(span[offset..]);
        t.y = BitConverter.ToUInt16(span[(offset + 2)..]);
        t.x /= ushort.MaxValue;
        t.y /= ushort.MaxValue;
    }

    private static void MakeNormalizedU8VEC4(byte[] bytes, int offset, ref Color t) {
        t.r = bytes[offset+0] / (float)byte.MaxValue;
        t.g = bytes[offset+1] / (float)byte.MaxValue;
        t.b = bytes[offset+2] / (float)byte.MaxValue;
        t.a = bytes[offset+3] / (float)byte.MaxValue;
    }

    private static void MakeVEC2(byte[] bytes, int offset, ref Vector2 t) {
        Span<byte> span = bytes;
        t.x = BitConverter.ToSingle(span[offset..]);
        t.y = BitConverter.ToSingle(span[(offset + 4)..]);

        // Debug.Log(string.Format("NP: {0} {1} {2} | {3} {4} {5} {6}", t.x, t.y, 0, span[offset], span[offset+1], span[offset+2], span[offset+3]));
    }

    private static void MakeVEC3(byte[] bytes, int offset, ref Vector3 t) {
        Span<byte> span = bytes;
        t.x = BitConverter.ToSingle(span[offset..]);
        t.y = BitConverter.ToSingle(span[(offset + 4)..]);
        t.z = BitConverter.ToSingle(span[(offset + 8)..]);

        //Debug.Log(string.Format("NP: {0} {1} {2} {3} {4} {5} {6}", t.x, t.y, t.z, span[offset], span[offset+1], span[offset+2], span[offset+3]));
    }

    private static Vector2[] MakeNormalizedU16VEC2Array(ReadOnlyMemory<byte> data, int stride, int vertex_count) {
        var ret = new Vector2[vertex_count];

        var bytes = data.ToArray();

        int cursor = 0;
        for (int i = 0; i < vertex_count; i++) {
            MakeNormalizedU16VEC2(bytes, cursor, ref ret[i]);
            cursor += stride;
        }
        

        return ret;
    }

    private static Color[] MakeNormalizedU8VEC4Array(ReadOnlyMemory<byte> data, int stride, int vertex_count) {
        var ret = new Color[vertex_count];

        var bytes = data.ToArray();

        int cursor = 0;
        for (int i = 0; i < vertex_count; i++) {
            MakeNormalizedU8VEC4(bytes, cursor, ref ret[i]);
            cursor += stride;
        }

        return ret;
    }

    private static Vector2[] MakeVEC2Array(ReadOnlyMemory<byte> data, int stride, int vertex_count) {
        Debug.Log("Textures from vec2");
        var ret = new Vector2[vertex_count];

        var bytes = data.ToArray();

        int cursor = 0;
        for (int i = 0; i < vertex_count; i++) {
            MakeVEC2(bytes, cursor, ref ret[i]);
            cursor += stride;
        }

        return ret;
    }

    private static Vector3[] MakeVEC3Array(ReadOnlyMemory<byte> data, int stride, int vertex_count) {
        //Debug.Log(string.Format("MAKE V3: {0} {1} {2} {3}", offset, stride, vertex_count, data.Length));
        var ret = new Vector3[vertex_count];

        var bytes = data.ToArray();

        int cursor = 0;
        for (int i = 0; i < vertex_count; i++) {
            MakeVEC3(bytes, cursor, ref ret[i]);
            cursor += stride;
        }

        return ret;
    }

    private static void SetColor(Mesh mesh, ReadOnlyMemory<byte> data, GeometryAttrib att, int vertex_count) {
        if (att.format == "U8VEC4") {
            mesh.SetColors(MakeNormalizedU8VEC4Array(data, att.stride, vertex_count));
        } else {
            Debug.LogWarning("Unsupported color format " + att.format);
        }    
    }

    private static void SetTexture(Mesh mesh, ReadOnlyMemory<byte> data, GeometryAttrib att, int vertex_count) {
        // Debug.Log(string.Format("Texture info: {0} {1} stride {2} offset {3}", att.format, att.normalized, att.stride, att.offset));
        Vector2[] vec2array;

        if (att.format == "VEC2") {
            vec2array = MakeVEC2Array(data, att.stride, vertex_count);
        } else {
            vec2array = MakeNormalizedU16VEC2Array(data, att.stride, vertex_count);
        }  

        // for (int i = 0; i < vec2array.Length; i++)
        // {
        //     vec2array[i].y *= -1;
        // }

        mesh.SetUVs(0, vec2array); 
    }

    private static void SetNormal(Mesh mesh, ReadOnlyMemory<byte> data, GeometryAttrib att, int vertex_count) {
        mesh.SetNormals(MakeVEC3Array(data, att.stride, vertex_count));
    }

    private static void SetPosition(Mesh mesh, ReadOnlyMemory<byte> data, GeometryAttrib att, int vertex_count) {
        Debug.Log("Setting positions");

        var p = MakeVEC3Array(data, att.stride, vertex_count);

        // Debug.Log(string.Format("Setting positions: {0} {1}", p.Length, p[0]));

        mesh.SetVertices(p);

        // Debug.Log("HERE: " + mesh.vertices);
    }

    private void AddVertexDataSlow(NOODLESRoot root, Mesh new_mesh, CBORObject attributes, int vertex_count, Dictionary<int, List<GeometryAttrib>> attrib_stream_map)
    {
        Assert.IsTrue(BitConverter.IsLittleEndian);

        foreach (var stream_pack in attrib_stream_map) {
            foreach (var att in stream_pack.Value) {
                var view = (BufferViewComponent)root.GetNoodlesComponent(ComponentType.BufferView, att.buffer_view)!;
                var data = view.GetBytes();
                data = data[att.offset..];

                switch (att.semantic) {
                    case "POSITION":
                        SetPosition(new_mesh, data, att, vertex_count);
                        break;
                    case "NORMAL":
                        SetNormal(new_mesh, data, att, vertex_count);
                        break;
                    case "TEXTURE":
                        SetTexture(new_mesh, data, att, vertex_count);
                        break;
                    case "COLOR":
                        SetColor(new_mesh, data, att, vertex_count);
                        break;
                }
                
            }
        }
    }

    private static void AddVertexDataInterleaved(
        NOODLESRoot root,
        Mesh new_mesh, 
        CBORObject attributes, 
        int vertex_count, 
        Dictionary<int, List<GeometryAttrib>> attrib_stream_map,
        Dictionary<NooID, int> stream_map,
        Dictionary<int, int> stream_stride
        )
    {
        var cursor = 0;
        var attrib_descriptions = new VertexAttributeDescriptor[attributes.Count];

        foreach (var stream_pack in attrib_stream_map)
        {
            stream_pack.Value.Sort((a, b) => a.offset.CompareTo(b.offset));

            int pointer = 0;
            int running_stride = -1;

            foreach (var att in stream_pack.Value)
            {
                // we want each att to be packed to a previous attr.
                // and the strides should be the vertex stride.

                var u_format = FormatToFormat(att.format);

                if (running_stride < 0)
                {
                    running_stride = att.stride;
                }
                else
                {
                    // to make sure they are lined up, all att strides should be the same
                    Assert.AreEqual(running_stride, att.stride);
                }

                // Debug.Log(string.Format("NEXT ATTR {0} {1}", running_stride, att.offset));

                Assert.AreEqual(pointer, att.offset);

                attrib_descriptions[cursor] = new VertexAttributeDescriptor(
                    SemanticToAttribute(att.semantic),
                    u_format.Item1,
                    u_format.Item2,
                    stream: stream_pack.Key
                );

                pointer += FormatToStride(att.format);

                cursor += 1;
            }

        }

        new_mesh.SetVertexBufferParams(vertex_count, attrib_descriptions);

        foreach (var stream in stream_map)
        {
            var buffer_view = (BufferViewComponent)root.GetNoodlesComponent(ComponentType.BufferView, stream.Key)!;
            var bytes = buffer_view.GetBytes();

            var vertex_size = stream_stride[stream.Value];

            // here we do some magic
            SetMeshData(new_mesh, bytes.ToArray(), vertex_size, vertex_count, stream.Value);
        }
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new Geometry: " +  NooTools.name_from_content(content, "Geometry"));

        var patches = content["patches"];

        meshes = new Mesh[patches.Count];
        materials = new Material[patches.Count];

        for (int i = 0; i < patches.Count; i++)
        {
            bool ok = InstallPatch(i, root, patches[i]);
            if (!ok) return;
        }
    }

    public void OnDelete(NOODLESRoot root)
    {
        Debug.Log("Destroying geometry");
        foreach (var m in meshes)
        {
            Mesh.Destroy(m);
        }
    }
}

#endregion

#region Materials

public static class MaterialExtensions {
    static int render_type_id = Shader.PropertyToID("RenderType");
    static int src_blend_id = Shader.PropertyToID("_SrcBlend");
    static int dst_blend_id = Shader.PropertyToID("_DstBlend");
    static int z_write = Shader.PropertyToID("_ZWrite");

    public static void SetupQueueOverride(this Material material, int min, int max, int def) {
        if (material.renderQueue < min || material.renderQueue > max) {
            material.renderQueue = def;
        }
    }

    public static void SetOpaque(this Material material) {
        material.SetOverrideTag("RenderType", "");
        material.SetFloat(src_blend_id, (float) BlendMode.One);
        material.SetFloat(dst_blend_id, (float) BlendMode.Zero);
        material.SetFloat(z_write, 1.0f);
        material.DisableKeyword("_ALPHA_TEST_ON");
        material.DisableKeyword("_ALPHABLEND_ON");
        material.DisableKeyword("_ALPHAPREMULTIPLY_ON");
        material.renderQueue = -1;
        material.SetupQueueOverride(-1, (int)RenderQueue.AlphaTest - 1, -1);
    }

    public static void SetCutout(this Material material) {
        material.SetOverrideTag("RenderType", "TransparentCutout");
        material.SetFloat(src_blend_id, (float)BlendMode.One);
        material.SetFloat(dst_blend_id, (float)BlendMode.Zero);
        material.SetFloat(z_write, 1.0f);
        material.EnableKeyword("_ALPHATEST_ON");
        material.DisableKeyword("_ALPHABLEND_ON");
        material.DisableKeyword("_ALPHAPREMULTIPLY_ON");
        material.SetupQueueOverride((int)RenderQueue.AlphaTest, (int)RenderQueue.GeometryLast, (int)RenderQueue.AlphaTest);
    }

    public static void SetFade(this Material material) {
        material.SetOverrideTag("RenderType", "TransparentCutout");
        material.SetFloat(src_blend_id, (float)BlendMode.One);
        material.SetFloat(dst_blend_id, (float)BlendMode.Zero);
        material.SetFloat(z_write, 1.0f);
        material.EnableKeyword("_ALPHATEST_ON");
        material.DisableKeyword("_ALPHABLEND_ON");
        material.DisableKeyword("_ALPHAPREMULTIPLY_ON");
        material.SetupQueueOverride((int)RenderQueue.GeometryLast + 1, (int)RenderQueue.Overlay - 1, (int)RenderQueue.Transparent);
    }

    public static void SetTransparent(this Material material) {
        material.SetOverrideTag("RenderType", "Transparent");
        material.SetFloat(src_blend_id, (float) BlendMode.One);
        material.SetFloat(dst_blend_id, (float) BlendMode.OneMinusSrcColor);
        material.SetFloat(z_write, 0.0f);
        material.DisableKeyword("_ALPHA_TEST_ON");
        material.DisableKeyword("_ALPHABLEND_ON");
        material.DisableKeyword("_ALPHAPREMULTIPLY_ON");
        material.SetupQueueOverride((int)RenderQueue.GeometryLast + 1, (int)RenderQueue.Overlay - 1, (int)RenderQueue.Transparent);
    }
}

public struct TextureRef {
    public TextureComponent texture;
    // TODO complete
}

public class MaterialComponent : INoodlesComponent
{
    Material? material;

    readonly int base_color_id = Shader.PropertyToID("_BaseColor");
    readonly int base_color_map_id = Shader.PropertyToID("_BaseMap");
    readonly int metallic_id = Shader.PropertyToID("_Metallic");
    readonly int smoothness_id = Shader.PropertyToID("_Smoothness");

    public MaterialComponent() {
        material = null;
    }

    public Material Material() {
        if (material == null) {
            throw new NullReferenceException("Missing material");
        }
        return material;
    }

    public static TextureRef GetTextureRef(NOODLESRoot root, CBORObject obj) {
        return new TextureRef {
            texture = (TextureComponent)root.GetNoodlesComponent(ComponentType.Texture, NooID.FromCBOR(obj["texture"]))!,
        };
    }

    public void CommonUpdate(NOODLESRoot root, CBORObject content) {

        NooTools.ActionOnContent("pbr_info", content, (CBORObject obj) => {

            var color = NooTools.ArrayToColor(obj["base_color"]);
            var metallic = NooTools.TypedFromContent("metallic", content, 1.0f);
            var roughness = NooTools.TypedFromContent("roughness", content, 1.0f);

            material!.SetColor(base_color_id, color);
            material!.SetFloat(metallic_id, metallic);
            material!.SetFloat(smoothness_id, 1.0f - roughness); 

            NooTools.ActionOnContent("base_color_texture", obj, (CBORObject value) => {
                Debug.Log("Found texture for material");
                var tex_ref = GetTextureRef(root, value);
                if (tex_ref.texture != null) {
                    material.SetTexture(base_color_map_id, tex_ref.texture.GetTexture());
                    material.SetTextureScale(base_color_map_id, new Vector2(1, -1));
                    material.SetTextureOffset(base_color_map_id, new Vector2(0, 1));
                } else {
                    Debug.Log("Texture is null!");
                }
            });
        });

        NooTools.ActionOnContent("use_alpha", content, (CBORObject value) => {
            material!.SetCutout();
        });
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new Material: " +  NooTools.name_from_content(content, "Material"));

        material = new Material(root.newMaterialShader);

        CommonUpdate(root, content);
    }

    public void OnDelete(NOODLESRoot root)
    {
        Debug.Log("Destroying material");
        UnityEngine.Material.Destroy(material);
    }
}

#endregion

#region Image
public class ImageComponent : INoodlesComponent
{
    ReadOnlyMemory<byte> bytes;
    string cached_buffer = "";

    public ImageComponent() {
    }

    public ReadOnlyMemory<byte> GetBytes() {
        return bytes;
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new Image: " +  NooTools.name_from_content(content, "Image"));

        NooTools.ActionOnContent("buffer_source", content, (CBORObject value) => {
            var view_id = NooID.FromCBOR(value);
            var view = (BufferViewComponent)root.GetNoodlesComponent(ComponentType.BufferView, view_id)!;
            bytes = view.GetBytes();
        });

        NooTools.ActionOnContent("uri_source", content, (CBORObject value) => {
            cached_buffer = value.AsString();
            bytes = root.BufferCacheGet(cached_buffer);
        });
    }

    public void OnDelete(NOODLESRoot root)
    {
        if (cached_buffer.Length > 0) {
            root.BufferCacheRelease(cached_buffer);
        }
    }
}

#endregion

#region Texture
public class TextureComponent : INoodlesComponent
{
    Texture2D? texture;

    public TextureComponent() {
    }

    public Texture2D GetTexture() {
        if (texture != null){
            return texture;
        }
        throw new NullReferenceException("Missing texture");
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        Debug.Log("Creating new Texture: " +  NooTools.name_from_content(content, "Texture"));

        var image_id = NooID.FromCBOR(content["image"]);
        var image = (ImageComponent)root.GetNoodlesComponent(ComponentType.Image, image_id)!;
        texture = new Texture2D(2, 2); // dims will be resized
        texture.LoadImage(image.GetBytes().ToArray());
        texture.wrapMode = TextureWrapMode.Clamp;
    }

    public void OnDelete(NOODLESRoot root)
    {
    }
}

#endregion

#region Entity

class EntityComponent : INoodlesComponent
{
    GameObject? managed_object;

    readonly List<GameObject> sub_objects;

    public EntityComponent() {
        sub_objects = new();
    }

    public void CommonUpdate(NOODLESRoot root, in CBORObject content) {
        NooTools.ActionOnContent("parent", content, 
            (parent) => {
                var id = NooID.FromCBOR(parent);
                if (id.IsNull()) {
                    managed_object!.transform.parent = root.transform;
                } else {
                    var comp = root.GetNoodlesComponent(ComponentType.Entity, id);

                    if (comp is not null) {
                        managed_object!.transform.parent = ((EntityComponent)comp).managed_object!.transform;
                    }
                }
            }
        );

        NooTools.ActionOnContent("render_rep", content, 
            (value) => {
                RebuildChildren(root, value);
            }
        );

        NooTools.ActionOnContent("null_rep", content, 
            (value) => {
                ClearChildren();
            }
        );

        NooTools.ActionOnContent("transform", content, (CBORObject tf_array) => {
            var mat = new Matrix4x4();

            for (int column = 0; column < 4; column++) {
                for (int row = 0; row < 4; row++) {
                    mat[row, column] = tf_array[row + column * 4].AsSingle();
                }    
            }

            var position = mat.GetPosition();
            var pre_rotation = mat.rotation;
            Debug.Log(string.Format("BEFORE {0} {1}", position, pre_rotation));

            // so we have an issue where some mirrored rotations are properly mirroed...but flipped in, say, Y.
            // this is a valid mirrored rotation, but ruins the scene

            var rotation = new Quaternion(-pre_rotation.x, -pre_rotation.y, pre_rotation.z, pre_rotation.w);

            position.z *= -1;

            Debug.Log(string.Format("AFTER {0} {1} {2}", position, rotation, mat.lossyScale));

            var tf = managed_object!.transform;
            tf.SetLocalPositionAndRotation(position, rotation);
            tf.localScale = mat.lossyScale;
        });

        NooTools.ActionOnContent("visible", content, 
            (value) => {
                var b = value.AsBoolean();
                Debug.Log("Setting visible " + b);
                managed_object!.SetActive(value.AsBoolean());
            }
        );
    }



    void RebuildChildren(NOODLESRoot root, CBORObject render_info) {
        ClearChildren();

        var mesh_id = NooID.FromCBOR(render_info["mesh"]);

        var geom_comp = (GeometryComponent)root.GetNoodlesComponent(ComponentType.Geometry, mesh_id)!;

        var meshes = geom_comp.MeshList();
        var materials = geom_comp.MaterialList();

        Debug.Log("Building children " + meshes.Length + " for " + managed_object!.name);

        for (int i = 0; i < meshes.Length; i++) {
            try {
                var mesh = meshes[i];
                var mat  = materials[i];
                Debug.Log("Adding mesh to subobject: " + mesh);
                var sub_obj = new GameObject(string.Format("Submesh {0} for {1}", i, managed_object!.name));

                sub_obj.transform.parent = managed_object!.transform;
                sub_obj.transform.localScale = new Vector3(1,1,-1);

                sub_obj.AddComponent<MeshFilter>().sharedMesh = mesh;
                sub_obj.AddComponent<MeshRenderer>().sharedMaterial = mat;

                sub_objects.Add(sub_obj);
            } catch (Exception e) {
                Debug.LogException(e);
            }
        }
    }

    void ClearChildren() {
        Debug.Log("Clearing for " + managed_object!.name);
        foreach (var child in sub_objects) {
            GameObject.Destroy(child);
        }
        sub_objects.Clear();
    }

    public void OnCreate(NOODLESRoot root, CBORObject content)
    {
        managed_object = new GameObject();
        Debug.Log("Creating new entity");
        managed_object.name =  NooTools.name_from_content(content, "Entity");

        if (!content.ContainsKey("parent")) {
            managed_object!.transform.parent = root.transform;
        }

        root.FireOnEntityCreated(managed_object, content);

        CommonUpdate(root, content);
    }

    public void OnUpdate(NOODLESRoot root, CBORObject content) {
        Debug.Log("Updating component: ");

        CommonUpdate(root, content);

        if (managed_object != null){
            root.FireOnEntityUpdated(managed_object, content);
        }
    }

    public void OnDelete(NOODLESRoot root)
    {
        Debug.Log("Destroying entity");
        GameObject.Destroy(managed_object);
    }
}

#endregion