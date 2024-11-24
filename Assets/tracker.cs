using System.Collections.Generic;
using System.Text;
using TMPro;
using UnityEngine;
using UnityEngine.XR.MagicLeap;

public class MarkerTracking : MonoBehaviour
{
    // marker settings
    public float qrCodeMarkerSize; // size of the expected qr code, in meters
    private Dictionary<string, GameObject> _markers = new Dictionary<string, GameObject>();
    private ASCIIEncoding _asciiEncoder = new System.Text.ASCIIEncoding();

    // the object that will be instantiated on the marker
    public GameObject trackerObject;

    private void Start()
    {
        // create a tracker settings object with variables defined above
        MLMarkerTracker.TrackerSettings trackerSettings = MLMarkerTracker.TrackerSettings.Create(true, MLMarkerTracker.MarkerType.QR, qrCodeMarkerSize);

        // start marker tracking with tracker settings object
        _ = MLMarkerTracker.SetSettingsAsync(trackerSettings);
    }

    // subscribe to the event that detects markers
    private void OnEnable()
    {
        MLMarkerTracker.OnMLMarkerTrackerResultsFound += OnTrackerResultsFound;
    }

    // when the marker is detected...
    private void OnTrackerResultsFound(MLMarkerTracker.MarkerData data)
    {
        string id = "";

        if (data.Type == MLMarkerTracker.MarkerType.QR)
        {
            id = _asciiEncoder.GetString(data.BinaryData.Data, 0, data.BinaryData.Data.Length);
        }

        if (!string.IsNullOrEmpty(id))
        {
            if (_markers.ContainsKey(id))
            {
                GameObject marker = _markers[id];

                // Reposition the marker
                //marker.transform.position = data.Pose.position;
                //marker.transform.rotation = data.Pose.rotation;
            }
            else
            {
                //Create an origin marker
                GameObject marker = Instantiate(trackerObject, data.Pose.position, data.Pose.rotation);
                marker.transform.GetChild(4).gameObject.GetComponent<TextMeshPro>().text = id;
                marker.transform.up = Vector3.up;
                GameObject tmp = Instantiate(trackerObject, data.Pose.position + new Vector3(0.1f,0.1f,0f), this.transform.rotation);

                // Position the marker
                //marker.transform.position = data.Pose.position;
                marker.transform.rotation = data.Pose.rotation * Quaternion.Euler(-90,0,180);

                this.transform.position = data.Pose.position;
                this.transform.rotation = marker.transform.rotation;

                _markers.Add(id, marker);
            }
        }

        // stop scanning after object has been instantiated
        //_ = MLMarkerTracker.StopScanningAsync();

    }
}