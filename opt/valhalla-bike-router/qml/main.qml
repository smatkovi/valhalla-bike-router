/**
 * Bike Router for MeeGo Harmattan
 * Version 2.7.0 - Offline Routing + Download
 */

import QtQuick 1.1
import com.nokia.meego 1.1
import com.nokia.extras 1.1

PageStackWindow {
    id: appWindow
    showStatusBar: false
    showToolBar: false
    initialPage: mainPage
    
    // Theme
    property bool darkTheme: false
    
    // Theme colors
    property color bgColor: darkTheme ? "#000000" : "#FFFFFF"
    property color cardColor: darkTheme ? "#1a1a1a" : "#F5F5F5"
    property color textColor: darkTheme ? "#FFFFFF" : "#333333"
    property color textSecondary: darkTheme ? "#888888" : "#666666"
    property color accentColor: "#4a90d9"
    property color headerColor: darkTheme ? "#1a1a1a" : "#4a90d9"
    property color headerTextColor: darkTheme ? "#4a90d9" : "#FFFFFF"
    property color inputBg: darkTheme ? "#333333" : "#FFFFFF"
    property color inputBorder: darkTheme ? "#444444" : "#CCCCCC"
    
    // Location data
    property variant fromLocation: null
    property variant toLocation: null
    property variant currentRoute: null
    
    // Settings
    property string selectedBicycleType: "Mountain"
    property string selectedBackend: "local"
    property real useRoads: 0.5
    property real useHills: 0.5
    property bool avoidCars: false
    
    // History and Favorites
    property variant historyList: []
    property variant favoritesList: []
    property string pickerTarget: ""
    
    // Loading state
    property bool loading: false
    
    // Bicycle types
    property variant bicycleTypes: [
        {id: "Mountain", name: "Mountain Bike"},
        {id: "Road", name: "Road Bike"},
        {id: "Hybrid", name: "City/Hybrid"},
        {id: "Cross", name: "Cyclocross"}
    ]
    
    // Routing backends
    property variant backends: [
        {id: "local", name: "Lokal (Offline)"},
        {id: "valhalla", name: "Valhalla (FOSSGIS)"},
        {id: "ors", name: "OpenRouteService"},
        {id: "osrm", name: "OSRM"}
    ]
    
    Component.onCompleted: {
        console.log("=== Bike Router 3.0.53 Starting ===")
        networkHelper.locationsReady.connect(onLocationsReady)
        networkHelper.routeReady.connect(onRouteReady)
        networkHelper.routeFileWritten.connect(onRouteFileWritten)
        networkHelper.errorOccurred.connect(onErrorOccurred)
        loadSettings()
        loadHistory()
        loadFavorites()
    }
    
    // ==================== Data Functions ====================
    
    function loadSettings() {
        try {
            var settings = JSON.parse(networkHelper.getSettings())
            if (settings.bicycleType) selectedBicycleType = settings.bicycleType
            if (settings.backend) selectedBackend = settings.backend
            if (settings.useRoads !== undefined) useRoads = settings.useRoads
            if (settings.useHills !== undefined) useHills = settings.useHills
            if (settings.avoidCars !== undefined) avoidCars = settings.avoidCars
            if (settings.darkTheme !== undefined) darkTheme = settings.darkTheme
        } catch (e) {
            console.log("Error loading settings: " + e)
        }
    }
    
    function saveSettings() {
        var settings = {
            bicycleType: selectedBicycleType,
            backend: selectedBackend,
            useRoads: useRoads,
            useHills: useHills,
            avoidCars: avoidCars,
            darkTheme: darkTheme
        }
        networkHelper.saveSettings(JSON.stringify(settings))
    }
    
    function loadHistory() {
        try {
            historyList = JSON.parse(networkHelper.getHistory())
        } catch (e) {
            historyList = []
        }
    }
    
    function loadFavorites() {
        try {
            favoritesList = JSON.parse(networkHelper.getFavorites())
        } catch (e) {
            favoritesList = []
        }
    }
    
    function clearHistory() {
        networkHelper.clearHistory()
        historyList = []
    }
    
    function addFavorite(name, location) {
        networkHelper.addToFavorites(name, JSON.stringify(location))
        loadFavorites()
    }
    
    function removeFavorite(location) {
        networkHelper.removeFromFavorites(location)
        loadFavorites()
    }
    
    function openPicker(target) {
        pickerTarget = target
        searchResultsModel.clear()
        searchField.text = ""
        pageStack.push(pickerPage)
    }
    
    function selectLocation(loc) {
        if (pickerTarget === "from") {
            fromLocation = loc
        } else if (pickerTarget === "to") {
            toLocation = loc
        }
        networkHelper.addToHistory(JSON.stringify(loc))
        loadHistory()
        pageStack.pop()
    }
    
    function searchRoute() {
        if (!fromLocation || !toLocation) return
        
        loading = true
        currentRoute = null
        
        networkHelper.searchRoute(
            fromLocation.lat.toString(),
            fromLocation.lng.toString(),
            toLocation.lat.toString(),
            toLocation.lng.toString(),
            selectedBicycleType,
            useRoads.toString(),
            useHills.toString(),
            selectedBackend,
            avoidCars.toString()
        )
    }
    
    function showInMaps() {
        if (!currentRoute) return
        
        var routeData = {
            polyline: currentRoute.polyline,
            start: currentRoute.start,
            end: currentRoute.end
        }
        networkHelper.openInMaps(JSON.stringify(routeData))
    }
    
    function getBicycleTypeName(id) {
        for (var i = 0; i < bicycleTypes.length; i++) {
            if (bicycleTypes[i].id === id) return bicycleTypes[i].name
        }
        return id
    }
    
    function getBackendName(id) {
        for (var i = 0; i < backends.length; i++) {
            if (backends[i].id === id) return backends[i].name
        }
        return id
    }
    
    function showBanner(msg) {
        banner.text = msg
        banner.show()
    }
    
    // ==================== Signal Handlers ====================
    
    function onLocationsReady(response) {
        searchBusy.running = false
        searchResultsModel.clear()
        
        try {
            var result = JSON.parse(response)
            if (result.success && result.locations) {
                for (var i = 0; i < result.locations.length; i++) {
                    searchResultsModel.append(result.locations[i])
                }
            } else if (result.error) {
                showBanner("Error: " + result.error)
            }
        } catch (e) {
            console.log("Parse error: " + e)
        }
    }
    
    function onRouteReady(response) {
        loading = false
        
        try {
            var result = JSON.parse(response)
            if (result.success) {
                currentRoute = result
            } else {
                showBanner(result.error || "Route not found")
            }
        } catch (e) {
            showBanner("Error processing response")
        }
    }
    
    function onRouteFileWritten() {
        console.log("Route file written")
    }
    
    function onErrorOccurred(error) {
        showBanner(error)
    }
    
    // ==================== Main Page ====================
    
    Page {
        id: mainPage
        
        Rectangle {
            anchors.fill: parent
            color: bgColor
        }
        
        // Header
        Rectangle {
            id: mainHeader
            width: parent.width
            height: 72
            color: headerColor
            
            Text {
                anchors.left: parent.left
                anchors.leftMargin: 16
                anchors.verticalCenter: parent.verticalCenter
                text: "Bike Router"
                font.pixelSize: 32
                font.bold: true
                color: headerTextColor
            }
            
            Row {
                anchors.right: parent.right
                anchors.rightMargin: 8
                anchors.verticalCenter: parent.verticalCenter
                spacing: 4
                
                // Download/Maps button
                Rectangle {
                    width: 48; height: 48; radius: 24
                    color: downloadMA.pressed ? "#00000022" : "transparent"
                    
                    Text {
                        anchors.centerIn: parent
                        text: "▼"
                        font.pixelSize: 22
                        color: headerTextColor
                    }
                    
                    MouseArea {
                        id: downloadMA
                        anchors.fill: parent
                        onClicked: {
                            browseRegions("")
                            pageStack.push(downloadPage)
                        }
                    }
                }
                
                // Theme toggle
                Rectangle {
                    width: 48; height: 48; radius: 24
                    color: themeMA.pressed ? "#00000022" : "transparent"
                    
                    Text {
                        anchors.centerIn: parent
                        text: darkTheme ? "☀" : "☾"
                        font.pixelSize: 24
                        color: headerTextColor
                    }
                    
                    MouseArea {
                        id: themeMA
                        anchors.fill: parent
                        onClicked: {
                            darkTheme = !darkTheme
                            saveSettings()
                        }
                    }
                }
                
                // Settings
                Rectangle {
                    width: 48; height: 48; radius: 24
                    color: settingsMA.pressed ? "#00000022" : "transparent"
                    
                    Text {
                        anchors.centerIn: parent
                        text: "⚙"
                        font.pixelSize: 24
                        color: headerTextColor
                    }
                    
                    MouseArea {
                        id: settingsMA
                        anchors.fill: parent
                        onClicked: pageStack.push(settingsPage)
                    }
                }
            }
        }
        
        Flickable {
            anchors.top: mainHeader.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            contentHeight: mainCol.height + 40
            clip: true
            
            Column {
                id: mainCol
                anchors {
                    left: parent.left
                    right: parent.right
                    top: parent.top
                    margins: 16
                }
                spacing: 16
                
                // Bike type & Backend info
                Row {
                    spacing: 8
                    
                    Text {
                        text: "🚴 " + getBicycleTypeName(selectedBicycleType)
                        font.pixelSize: 16
                        color: accentColor
                    }
                    
                    Text {
                        text: "•"
                        font.pixelSize: 16
                        color: textSecondary
                    }
                    
                    Text {
                        text: getBackendName(selectedBackend)
                        font.pixelSize: 16
                        color: textSecondary
                    }
                }
                
                // FROM field
                Column {
                    width: parent.width
                    spacing: 4
                    
                    Text {
                        text: "From:"
                        font.pixelSize: 18
                        color: textColor
                    }
                    
                    Rectangle {
                        width: parent.width
                        height: 50
                        color: inputBg
                        border.width: 1
                        border.color: inputBorder
                        radius: 8
                        
                        Text {
                            anchors.left: parent.left
                            anchors.leftMargin: 12
                            anchors.right: clearFromBtn.left
                            anchors.rightMargin: 8
                            anchors.verticalCenter: parent.verticalCenter
                            text: fromLocation ? fromLocation.name : "Select start location..."
                            font.pixelSize: 16
                            color: fromLocation ? textColor : textSecondary
                            elide: Text.ElideRight
                        }
                        
                        // Clear button
                        Rectangle {
                            id: clearFromBtn
                            visible: fromLocation !== null
                            anchors.right: parent.right
                            anchors.rightMargin: 8
                            anchors.verticalCenter: parent.verticalCenter
                            width: 32; height: 32; radius: 16
                            color: clearFromMA.pressed ? "#CC0000" : "#999999"
                            
                            Text {
                                anchors.centerIn: parent
                                text: "✕"
                                font.pixelSize: 16
                                color: "white"
                            }
                            
                            MouseArea {
                                id: clearFromMA
                                anchors.fill: parent
                                onClicked: fromLocation = null
                            }
                        }
                        
                        MouseArea {
                            anchors.fill: parent
                            anchors.rightMargin: fromLocation ? 45 : 0
                            onClicked: openPicker("from")
                        }
                    }
                }
                
                // Swap button
                Rectangle {
                    width: 44; height: 44
                    anchors.horizontalCenter: parent.horizontalCenter
                    color: swapMA.pressed ? accentColor : cardColor
                    radius: 22
                    border.width: 1
                    border.color: inputBorder
                    
                    Text {
                        anchors.centerIn: parent
                        text: "⇅"
                        font.pixelSize: 22
                        color: swapMA.pressed ? "white" : textSecondary
                    }
                    
                    MouseArea {
                        id: swapMA
                        anchors.fill: parent
                        onClicked: {
                            var temp = fromLocation
                            fromLocation = toLocation
                            toLocation = temp
                        }
                    }
                }
                
                // TO field
                Column {
                    width: parent.width
                    spacing: 4
                    
                    Text {
                        text: "To:"
                        font.pixelSize: 18
                        color: textColor
                    }
                    
                    Rectangle {
                        width: parent.width
                        height: 50
                        color: inputBg
                        border.width: 1
                        border.color: inputBorder
                        radius: 8
                        
                        Text {
                            anchors.left: parent.left
                            anchors.leftMargin: 12
                            anchors.right: clearToBtn.left
                            anchors.rightMargin: 8
                            anchors.verticalCenter: parent.verticalCenter
                            text: toLocation ? toLocation.name : "Select destination..."
                            font.pixelSize: 16
                            color: toLocation ? textColor : textSecondary
                            elide: Text.ElideRight
                        }
                        
                        // Clear button
                        Rectangle {
                            id: clearToBtn
                            visible: toLocation !== null
                            anchors.right: parent.right
                            anchors.rightMargin: 8
                            anchors.verticalCenter: parent.verticalCenter
                            width: 32; height: 32; radius: 16
                            color: clearToMA.pressed ? "#CC0000" : "#999999"
                            
                            Text {
                                anchors.centerIn: parent
                                text: "✕"
                                font.pixelSize: 16
                                color: "white"
                            }
                            
                            MouseArea {
                                id: clearToMA
                                anchors.fill: parent
                                onClicked: toLocation = null
                            }
                        }
                        
                        MouseArea {
                            anchors.fill: parent
                            anchors.rightMargin: toLocation ? 45 : 0
                            onClicked: openPicker("to")
                        }
                    }
                }
                
                // Search button
                Rectangle {
                    width: parent.width
                    height: 56
                    color: (fromLocation && toLocation && !loading) ? 
                           (searchBtnMA.pressed ? "#3d7abd" : accentColor) : cardColor
                    radius: 8
                    
                    Text {
                        anchors.centerIn: parent
                        text: loading ? "Searching..." : "Find Route"
                        font.pixelSize: 20
                        font.bold: true
                        color: (fromLocation && toLocation) ? "white" : textSecondary
                    }
                    
                    MouseArea {
                        id: searchBtnMA
                        anchors.fill: parent
                        enabled: fromLocation !== null && toLocation !== null && !loading
                        onClicked: searchRoute()
                    }
                }
                
                // Loading indicator
                BusyIndicator {
                    anchors.horizontalCenter: parent.horizontalCenter
                    running: loading
                    visible: loading
                    platformStyle: BusyIndicatorStyle { size: "large" }
                }
                
                // Route result
                Rectangle {
                    visible: currentRoute !== null
                    width: parent.width
                    height: routeResultCol.height + 24
                    color: "#4CAF50"
                    radius: 8
                    
                    Column {
                        id: routeResultCol
                        anchors {
                            left: parent.left
                            right: parent.right
                            top: parent.top
                            margins: 12
                        }
                        spacing: 8
                        
                        Row {
                            spacing: 8
                            Text {
                                text: "✓ Route found"
                                font.pixelSize: 18
                                font.bold: true
                                color: "white"
                            }
                            Text {
                                text: "(" + (currentRoute ? currentRoute.backend_name : "") + ")"
                                font.pixelSize: 14
                                color: "#C8E6C9"
                            }
                        }
                        
                        Row {
                            spacing: 32
                            
                            Column {
                                Text {
                                    text: "Distance"
                                    font.pixelSize: 14
                                    color: "#C8E6C9"
                                }
                                Text {
                                    text: currentRoute ? currentRoute.distance_text : ""
                                    font.pixelSize: 28
                                    font.bold: true
                                    color: "white"
                                }
                            }
                            
                            Column {
                                Text {
                                    text: "Duration"
                                    font.pixelSize: 14
                                    color: "#C8E6C9"
                                }
                                Text {
                                    text: currentRoute ? currentRoute.duration_text : ""
                                    font.pixelSize: 28
                                    font.bold: true
                                    color: "white"
                                }
                            }
                        }
                        
                        // Road statistics (only for local backend)
                        Row {
                            spacing: 24
                            visible: currentRoute ? (currentRoute.car_distance_text ? true : false) : false
                            
                            Text {
                                text: currentRoute ? (currentRoute.car_distance_text || "") : ""
                                font.pixelSize: 13
                                color: "#FFCDD2"  // Light red for car distance
                            }
                            
                            Text {
                                text: currentRoute ? (currentRoute.cycleway_distance_text || "") : ""
                                font.pixelSize: 13
                                color: "#C8E6C9"  // Light green for cycleway
                            }
                        }
                        
                        Text {
                            text: currentRoute ? currentRoute.bicycle_name : ""
                            font.pixelSize: 14
                            color: "#C8E6C9"
                        }
                        
                        Item { width: 1; height: 4 }
                        
                        Rectangle {
                            width: parent.width
                            height: 48
                            color: showMapMA.pressed ? "#388E3C" : "#2E7D32"
                            radius: 6
                            
                            Text {
                                anchors.centerIn: parent
                                text: "Show in Nokia Maps"
                                font.pixelSize: 18
                                font.bold: true
                                color: "white"
                            }
                            
                            MouseArea {
                                id: showMapMA
                                anchors.fill: parent
                                onClicked: showInMaps()
                            }
                        }
                    }
                }
            }
        }
    }
    
    // ==================== Picker Page ====================
    
    Page {
        id: pickerPage
        
        Rectangle {
            anchors.fill: parent
            color: bgColor
        }
        
        // Header
        Rectangle {
            id: pickerHeader
            width: parent.width
            height: 72
            color: headerColor
            
            Text {
                anchors.left: parent.left
                anchors.leftMargin: 16
                anchors.verticalCenter: parent.verticalCenter
                text: pickerTarget === "from" ? "Select Start" : "Select Destination"
                font.pixelSize: 26
                font.bold: true
                color: headerTextColor
            }
            
            Rectangle {
                anchors.right: parent.right
                anchors.rightMargin: 8
                anchors.verticalCenter: parent.verticalCenter
                width: 48; height: 48; radius: 24
                color: pickerBackMA.pressed ? "#00000022" : "transparent"
                
                Text {
                    anchors.centerIn: parent
                    text: "✕"
                    font.pixelSize: 24
                    color: headerTextColor
                }
                
                MouseArea {
                    id: pickerBackMA
                    anchors.fill: parent
                    onClicked: pageStack.pop()
                }
            }
        }
        
        // Search field
        Rectangle {
            id: searchBox
            anchors.top: pickerHeader.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.margins: 16
            anchors.topMargin: 16
            height: 50
            color: inputBg
            border.width: 1
            border.color: inputBorder
            radius: 8
            
            TextField {
                id: searchField
                anchors.left: parent.left
                anchors.right: searchClearBtn.left
                anchors.top: parent.top
                anchors.bottom: parent.bottom
                anchors.margins: 4
                anchors.rightMargin: 0
                placeholderText: "Enter address or place..."
                platformStyle: TextFieldStyle {
                    background: ""
                    backgroundSelected: ""
                    backgroundDisabled: ""
                    backgroundError: ""
                    textColor: textColor
                }
                
                Keys.onReturnPressed: {
                    if (text.length >= 2) {
                        searchBusy.running = true
                        networkHelper.searchLocations(text)
                    }
                }
            }
            
            // Clear text button
            Rectangle {
                id: searchClearBtn
                visible: searchField.text.length > 0
                anchors.right: searchGoBtn.left
                anchors.rightMargin: 4
                anchors.verticalCenter: parent.verticalCenter
                width: 36; height: 36
                radius: 18
                color: searchClearMA.pressed ? "#CC0000" : "#999999"
                
                Text {
                    anchors.centerIn: parent
                    text: "✕"
                    font.pixelSize: 16
                    color: "white"
                }
                
                MouseArea {
                    id: searchClearMA
                    anchors.fill: parent
                    onClicked: searchField.text = ""
                }
            }
            
            // Search button
            Rectangle {
                id: searchGoBtn
                anchors.right: parent.right
                anchors.rightMargin: 6
                anchors.verticalCenter: parent.verticalCenter
                width: 42; height: 42
                radius: 6
                color: searchField.text.length >= 2 ? 
                       (searchBtnFieldMA.pressed ? "#3d7abd" : accentColor) : cardColor
                
                Text {
                    anchors.centerIn: parent
                    text: "→"
                    font.pixelSize: 22
                    color: searchField.text.length >= 2 ? "white" : textSecondary
                }
                
                MouseArea {
                    id: searchBtnFieldMA
                    anchors.fill: parent
                    enabled: searchField.text.length >= 2
                    onClicked: {
                        searchBusy.running = true
                        networkHelper.searchLocations(searchField.text)
                    }
                }
            }
        }
        
        BusyIndicator {
            id: searchBusy
            anchors.top: searchBox.bottom
            anchors.topMargin: 16
            anchors.horizontalCenter: parent.horizontalCenter
            running: false
            visible: running
        }
        
        Flickable {
            anchors.top: searchBox.bottom
            anchors.topMargin: 8
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            contentHeight: pickerCol.height + 40
            clip: true
            
            Column {
                id: pickerCol
                width: parent.width
                spacing: 1
                
                // Search Results
                Repeater {
                    model: ListModel { id: searchResultsModel }
                    
                    Rectangle {
                        width: pickerCol.width
                        height: 60
                        color: searchItemMA.pressed ? (darkTheme ? "#333333" : "#E0E0E0") : cardColor
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 12
                            spacing: 12
                            
                            Text {
                                text: "📍"
                                font.pixelSize: 20
                                anchors.verticalCenter: parent.verticalCenter
                            }
                            
                            Text {
                                text: name || ""
                                font.pixelSize: 16
                                color: textColor
                                anchors.verticalCenter: parent.verticalCenter
                                elide: Text.ElideRight
                                width: parent.width - 50
                            }
                        }
                        
                        MouseArea {
                            id: searchItemMA
                            anchors.fill: parent
                            onClicked: selectLocation({name: name, lat: lat, lng: lng})
                        }
                    }
                }
                
                Item { width: 1; height: 16; visible: searchResultsModel.count === 0 }
                
                // Favorites Section
                Text {
                    visible: favoritesList.length > 0 && searchResultsModel.count === 0
                    anchors.left: parent.left
                    anchors.leftMargin: 16
                    text: "★ Favorites"
                    font.pixelSize: 18
                    font.bold: true
                    color: "#FFC107"
                }
                
                Item { width: 1; height: 8; visible: favoritesList.length > 0 && searchResultsModel.count === 0 }
                
                Repeater {
                    model: searchResultsModel.count === 0 ? favoritesList : []
                    
                    Rectangle {
                        width: pickerCol.width
                        height: 60
                        color: favItemMA.pressed ? (darkTheme ? "#333333" : "#E0E0E0") : cardColor
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 12
                            spacing: 12
                            
                            Text {
                                text: "★"
                                font.pixelSize: 22
                                color: "#FFC107"
                                anchors.verticalCenter: parent.verticalCenter
                            }
                            
                            Column {
                                anchors.verticalCenter: parent.verticalCenter
                                width: parent.width - 50
                                
                                Text {
                                    text: modelData.name || ""
                                    font.pixelSize: 16
                                    font.bold: true
                                    color: textColor
                                    elide: Text.ElideRight
                                    width: parent.width
                                }
                                
                                Text {
                                    visible: modelData.name !== modelData.location
                                    text: modelData.location || ""
                                    font.pixelSize: 13
                                    color: textSecondary
                                    elide: Text.ElideRight
                                    width: parent.width
                                }
                            }
                        }
                        
                        MouseArea {
                            id: favItemMA
                            anchors.fill: parent
                            onClicked: selectLocation({name: modelData.location, lat: modelData.lat, lng: modelData.lng})
                            onPressAndHold: {
                                removeFavorite(modelData.location)
                                showBanner("Favorite removed")
                            }
                        }
                    }
                }
                
                Item { width: 1; height: 20; visible: favoritesList.length > 0 && searchResultsModel.count === 0 }
                
                // History Section
                Row {
                    visible: historyList.length > 0 && searchResultsModel.count === 0
                    anchors.left: parent.left
                    anchors.right: parent.right
                    anchors.margins: 16
                    
                    Text {
                        text: "🕐 Recent"
                        font.pixelSize: 18
                        font.bold: true
                        color: accentColor
                    }
                    
                    Item { width: parent.width - 160; height: 1 }
                    
                    Rectangle {
                        width: 70
                        height: 28
                        radius: 4
                        color: clearHistMA.pressed ? "#CC0000" : "#E53935"
                        
                        Text {
                            anchors.centerIn: parent
                            text: "Clear"
                            font.pixelSize: 13
                            color: "white"
                        }
                        
                        MouseArea {
                            id: clearHistMA
                            anchors.fill: parent
                            onClicked: {
                                clearHistory()
                                showBanner("History cleared")
                            }
                        }
                    }
                }
                
                Item { width: 1; height: 8; visible: historyList.length > 0 && searchResultsModel.count === 0 }
                
                Repeater {
                    model: searchResultsModel.count === 0 ? historyList : []
                    
                    Rectangle {
                        width: pickerCol.width
                        height: 56
                        color: histItemMA.pressed ? (darkTheme ? "#333333" : "#E0E0E0") : cardColor
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 12
                            spacing: 12
                            
                            Text {
                                text: "🕐"
                                font.pixelSize: 18
                                anchors.verticalCenter: parent.verticalCenter
                            }
                            
                            Text {
                                text: modelData.name || ""
                                font.pixelSize: 15
                                color: textColor
                                anchors.verticalCenter: parent.verticalCenter
                                elide: Text.ElideRight
                                width: parent.width - 100
                            }
                            
                            // Add to favorites
                            Rectangle {
                                width: 36; height: 36
                                radius: 4
                                color: addFavMA.pressed ? "#E65100" : "#FF9800"
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Text {
                                    anchors.centerIn: parent
                                    text: "★"
                                    font.pixelSize: 18
                                    color: "white"
                                }
                                
                                MouseArea {
                                    id: addFavMA
                                    anchors.fill: parent
                                    onClicked: {
                                        addFavorite(modelData.name, modelData)
                                        showBanner("Added to favorites")
                                    }
                                }
                            }
                        }
                        
                        MouseArea {
                            id: histItemMA
                            anchors.fill: parent
                            anchors.rightMargin: 50
                            onClicked: selectLocation(modelData)
                        }
                    }
                }
                
                // Empty state
                Column {
                    visible: historyList.length === 0 && favoritesList.length === 0 && searchResultsModel.count === 0
                    anchors.horizontalCenter: parent.horizontalCenter
                    spacing: 10
                    
                    Item { width: 1; height: 60 }
                    
                    Text {
                        anchors.horizontalCenter: parent.horizontalCenter
                        text: "🔍"
                        font.pixelSize: 48
                    }
                    
                    Text {
                        anchors.horizontalCenter: parent.horizontalCenter
                        text: "Search for a location"
                        font.pixelSize: 18
                        color: textSecondary
                    }
                }
            }
        }
    }
    
    // ==================== Settings Page ====================
    
    Page {
        id: settingsPage
        
        Rectangle {
            anchors.fill: parent
            color: bgColor
        }
        
        Rectangle {
            id: settingsHeader
            width: parent.width
            height: 72
            color: headerColor
            
            Text {
                anchors.left: parent.left
                anchors.leftMargin: 16
                anchors.verticalCenter: parent.verticalCenter
                text: "Settings"
                font.pixelSize: 26
                font.bold: true
                color: headerTextColor
            }
            
            Rectangle {
                anchors.right: parent.right
                anchors.rightMargin: 8
                anchors.verticalCenter: parent.verticalCenter
                width: 48; height: 48; radius: 24
                color: settingsBackMA.pressed ? "#00000022" : "transparent"
                
                Text {
                    anchors.centerIn: parent
                    text: "✕"
                    font.pixelSize: 24
                    color: headerTextColor
                }
                
                MouseArea {
                    id: settingsBackMA
                    anchors.fill: parent
                    onClicked: {
                        saveSettings()
                        pageStack.pop()
                    }
                }
            }
        }
        
        Flickable {
            anchors.top: settingsHeader.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            contentHeight: settingsCol.height + 40
            clip: true
            
            Column {
                id: settingsCol
                width: parent.width
                spacing: 0
                
                Item { width: 1; height: 16 }
                
                // Theme toggle
                Rectangle {
                    width: parent.width
                    height: 60
                    color: cardColor
                    
                    Row {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 16
                        
                        Text {
                            text: "Dark Theme"
                            font.pixelSize: 18
                            color: textColor
                            anchors.verticalCenter: parent.verticalCenter
                        }
                        
                        Item { width: parent.width - 200; height: 1 }
                        
                        Switch {
                            anchors.verticalCenter: parent.verticalCenter
                            checked: darkTheme
                            onCheckedChanged: {
                                darkTheme = checked
                                saveSettings()
                            }
                        }
                    }
                }
                
                Item { width: 1; height: 24 }
                
                // Routing Backend
                Text {
                    anchors.left: parent.left
                    anchors.leftMargin: 16
                    text: "Routing Backend"
                    font.pixelSize: 14
                    color: accentColor
                }
                
                Item { width: 1; height: 8 }
                
                Repeater {
                    model: backends
                    
                    Rectangle {
                        width: parent.width
                        height: 56
                        color: backendMA.pressed ? (darkTheme ? "#333333" : "#E0E0E0") : 
                               (selectedBackend === modelData.id ? (darkTheme ? "#1B5E20" : "#C8E6C9") : cardColor)
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 16
                            spacing: 12
                            
                            Rectangle {
                                width: 24; height: 24
                                radius: 12
                                color: "transparent"
                                border.width: 2
                                border.color: selectedBackend === modelData.id ? "#4CAF50" : textSecondary
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Rectangle {
                                    anchors.centerIn: parent
                                    width: 12; height: 12
                                    radius: 6
                                    color: "#4CAF50"
                                    visible: selectedBackend === modelData.id
                                }
                            }
                            
                            Text {
                                text: modelData.name
                                font.pixelSize: 18
                                color: textColor
                                anchors.verticalCenter: parent.verticalCenter
                            }
                        }
                        
                        MouseArea {
                            id: backendMA
                            anchors.fill: parent
                            onClicked: selectedBackend = modelData.id
                        }
                    }
                }
                
                Item { width: 1; height: 24 }
                
                // Bicycle Type
                Text {
                    anchors.left: parent.left
                    anchors.leftMargin: 16
                    text: "Bicycle Type"
                    font.pixelSize: 14
                    color: accentColor
                }
                
                Item { width: 1; height: 8 }
                
                Repeater {
                    model: bicycleTypes
                    
                    Rectangle {
                        width: parent.width
                        height: 56
                        color: bikeMA.pressed ? (darkTheme ? "#333333" : "#E0E0E0") : 
                               (selectedBicycleType === modelData.id ? (darkTheme ? "#1B5E20" : "#C8E6C9") : cardColor)
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 16
                            spacing: 12
                            
                            Rectangle {
                                width: 24; height: 24
                                radius: 12
                                color: "transparent"
                                border.width: 2
                                border.color: selectedBicycleType === modelData.id ? "#4CAF50" : textSecondary
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Rectangle {
                                    anchors.centerIn: parent
                                    width: 12; height: 12
                                    radius: 6
                                    color: "#4CAF50"
                                    visible: selectedBicycleType === modelData.id
                                }
                            }
                            
                            Text {
                                text: modelData.name
                                font.pixelSize: 18
                                color: textColor
                                anchors.verticalCenter: parent.verticalCenter
                            }
                        }
                        
                        MouseArea {
                            id: bikeMA
                            anchors.fill: parent
                            onClicked: selectedBicycleType = modelData.id
                        }
                    }
                }
                
                Item { width: 1; height: 24 }
                
                // Valhalla-specific settings
                Text {
                    visible: selectedBackend === "valhalla"
                    anchors.left: parent.left
                    anchors.leftMargin: 16
                    text: "Valhalla Options"
                    font.pixelSize: 14
                    color: accentColor
                }
                
                Item { width: 1; height: 8; visible: selectedBackend === "valhalla" }
                
                Rectangle {
                    visible: selectedBackend === "valhalla"
                    width: parent.width
                    height: 80
                    color: cardColor
                    
                    Column {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 8
                        
                        Text {
                            text: "Road preference: " + (useRoads < 0.3 ? "Bike paths" : (useRoads > 0.7 ? "Roads" : "Balanced"))
                            font.pixelSize: 14
                            color: textSecondary
                        }
                        
                        Slider {
                            width: parent.width
                            minimumValue: 0.0
                            maximumValue: 1.0
                            value: useRoads
                            stepSize: 0.1
                            onValueChanged: useRoads = value
                        }
                    }
                }
                
                Rectangle {
                    visible: selectedBackend === "valhalla"
                    width: parent.width
                    height: 80
                    color: cardColor
                    
                    Column {
                        anchors.fill: parent
                        anchors.margins: 16
                        spacing: 8
                        
                        Text {
                            text: "Hill tolerance: " + (useHills < 0.3 ? "Avoid hills" : (useHills > 0.7 ? "Don't mind" : "Moderate"))
                            font.pixelSize: 14
                            color: textSecondary
                        }
                        
                        Slider {
                            width: parent.width
                            minimumValue: 0.0
                            maximumValue: 1.0
                            value: useHills
                            stepSize: 0.1
                            onValueChanged: useHills = value
                        }
                    }
                }
                
                // Avoid Cars Option
                Rectangle {
                    width: parent.width - 32
                    height: avoidCarsCol.height + 32
                    anchors.horizontalCenter: parent.horizontalCenter
                    color: cardColor
                    radius: 8
                    visible: selectedBackend === "local"
                    
                    Column {
                        id: avoidCarsCol
                        anchors.left: parent.left
                        anchors.right: parent.right
                        anchors.top: parent.top
                        anchors.margins: 16
                        spacing: 8
                        
                        Row {
                            width: parent.width
                            spacing: 12
                            
                            Text {
                                text: "Avoid Cars"
                                font.pixelSize: 14
                                color: textColor
                                anchors.verticalCenter: parent.verticalCenter
                            }
                            
                            Item { width: parent.width - 180 }
                            
                            Rectangle {
                                width: 52
                                height: 28
                                radius: 14
                                color: avoidCars ? "#4CAF50" : (darkTheme ? "#555" : "#ccc")
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Rectangle {
                                    width: 24
                                    height: 24
                                    radius: 12
                                    color: "white"
                                    x: avoidCars ? parent.width - width - 2 : 2
                                    anchors.verticalCenter: parent.verticalCenter
                                    
                                    Behavior on x { NumberAnimation { duration: 150 } }
                                }
                                
                                MouseArea {
                                    anchors.fill: parent
                                    onClicked: avoidCars = !avoidCars
                                }
                            }
                        }
                        
                        Text {
                            text: avoidCars ? "Prefers cycleways and car-free paths" : "Normal route planning"
                            font.pixelSize: 12
                            color: textSecondary
                            wrapMode: Text.WordWrap
                            width: parent.width
                        }
                    }
                }
                
                Item { width: 1; height: 32 }
                
                // About
                Rectangle {
                    width: parent.width - 32
                    height: aboutCol.height + 24
                    anchors.horizontalCenter: parent.horizontalCenter
                    color: cardColor
                    radius: 8
                    
                    Column {
                        id: aboutCol
                        anchors {
                            left: parent.left
                            right: parent.right
                            top: parent.top
                            margins: 12
                        }
                        spacing: 4
                        
                        Text {
                            text: "Bike Router v3.0.53"
                            font.pixelSize: 16
                            font.bold: true
                            color: textColor
                        }
                        
                        Text {
                            text: "Routing: Valhalla, OpenRouteService, OSRM"
                            font.pixelSize: 13
                            color: textSecondary
                        }
                        
                        Text {
                            text: "Offline: Local Valhalla Engine"
                            font.pixelSize: 13
                            color: textSecondary
                        }
                        
                        Text {
                            text: "Geocoding: Photon (Komoot)"
                            font.pixelSize: 13
                            color: textSecondary
                        }
                        
                        Text {
                            text: "Data: OpenStreetMap"
                            font.pixelSize: 13
                            color: textSecondary
                        }
                    }
                }
            }
        }
    }
    
    // Banner
    InfoBanner {
        id: banner
        timerShowTime: 3000
    }
    
    // ==================== Download Page ====================
    
    property variant browseItems: []
    property string browsePath: ""
    property variant breadcrumb: []
    property variant downloadStatus: ({})
    property int installedTileCount: 0
    
    function browseRegions(path) {
        browsePath = path || ""
        var result = networkHelper.runCommand("browse " + browsePath)
        try {
            var data = JSON.parse(result)
            if (data.items) {
                browseItems = data.items
                breadcrumb = data.breadcrumb || []
            }
        } catch (e) {
            console.log("Error browsing: " + e)
        }
        if (browsePath === "") {
            loadInstalledTiles()
        }
    }
    
    function goBack() {
        if (breadcrumb.length > 1) {
            // Go to parent of current
            browseRegions(breadcrumb[breadcrumb.length - 2].path)
        } else if (breadcrumb.length === 1) {
            // Go to root
            browseRegions("")
        }
    }
    
    function loadInstalledTiles() {
        var result = networkHelper.runCommand("tiles")
        try {
            var data = JSON.parse(result)
            installedTileCount = data.count || 0
        } catch (e) {
            console.log("Error loading tiles: " + e)
        }
    }
    
    function startDownload(regionId) {
        console.log("startDownload called with: " + regionId)
        banner.text = "Starting download: " + regionId
        banner.show()
        
        var result = networkHelper.runCommand("download " + regionId)
        console.log("Download command result: " + result)
        
        try {
            var data = JSON.parse(result)
            console.log("Parsed data success: " + data.success + " error: " + data.error)
            if (data.success) {
                downloadStatus[regionId] = {progress: 0, status: 'starting'}
                checkDownloadTimer.start()
                banner.text = "Download started for " + regionId
                banner.show()
            } else {
                banner.text = "Error: " + (data.error || "Unknown")
                banner.show()
            }
        } catch (e) {
            console.log("JSON parse error: " + e)
            banner.text = "Error: " + e + " - Result: " + result.substring(0, 100)
            banner.show()
        }
    }
    
    function startUpdate(regionId) {
        console.log("startUpdate called with: " + regionId)
        banner.text = "Checking updates: " + regionId
        banner.show()
        
        var result = networkHelper.runCommand("update " + regionId)
        console.log("Update command result: " + result)
        
        try {
            var data = JSON.parse(result)
            console.log("Parsed data success: " + data.success + " error: " + data.error)
            if (data.success) {
                downloadStatus[regionId] = {progress: 0, status: 'checking'}
                checkDownloadTimer.start()
                banner.text = "Update started for " + regionId
                banner.show()
            } else {
                banner.text = "Error: " + (data.error || "Unknown")
                banner.show()
            }
        } catch (e) {
            console.log("JSON parse error: " + e)
            banner.text = "Error: " + e
            banner.show()
        }
    }
    
    function checkDownloadStatus() {
        var result = networkHelper.runCommand("download_status")
        try {
            var data = JSON.parse(result)
            if (data.downloads) {
                // Force property update by creating new object
                // Also filter out old completed/error entries
                var newStatus = {}
                var now = Date.now()
                for (var key in data.downloads) {
                    var d = data.downloads[key]
                    // Only keep active downloads or recent completions
                    if (d.status !== 'complete' && d.status !== 'error' && d.status !== 'idle') {
                        newStatus[key] = d
                    }
                }
                downloadStatus = newStatus
                
                // Debug log
                for (var k in newStatus) {
                    var dl = newStatus[k]
                    console.log("Download " + k + ": " + dl.progress + "% - " + dl.status)
                }
                
                // Check if any downloads still in progress
                var anyActive = false
                for (var key2 in data.downloads) {
                    var dl2 = data.downloads[key2]
                    if (dl2.status !== 'complete' && dl2.status !== 'error' && dl2.status !== 'idle') {
                        anyActive = true
                    }
                    if (dl2.status === 'complete') {
                        banner.text = key2.split('/').pop() + " download complete!"
                        banner.show()
                        loadInstalledTiles()
                        // Refresh the browse list to show updated status
                        browseRegions(browsePath)
                    }
                }
                
                if (!anyActive) {
                    checkDownloadTimer.stop()
                }
            }
        } catch (e) {
            console.log("Error checking download: " + e)
        }
    }
    
    Timer {
        id: checkDownloadTimer
        interval: 1000
        repeat: true
        onTriggered: checkDownloadStatus()
    }
    
    Page {
        id: downloadPage
        
        Rectangle {
            anchors.fill: parent
            color: bgColor
        }
        
        Rectangle {
            id: downloadHeader
            width: parent.width
            height: 72
            color: headerColor
            
            // Back button (only if not at root)
            Rectangle {
                id: backBtn
                anchors.left: parent.left
                anchors.leftMargin: 8
                anchors.verticalCenter: parent.verticalCenter
                width: 48; height: 48; radius: 24
                color: backBtnMA.pressed ? "#00000022" : "transparent"
                visible: browsePath !== ""
                
                Text {
                    anchors.centerIn: parent
                    text: "<"
                    font.pixelSize: 28
                    font.bold: true
                    color: headerTextColor
                }
                
                MouseArea {
                    id: backBtnMA
                    anchors.fill: parent
                    onClicked: goBack()
                }
            }
            
            Column {
                anchors.left: backBtn.visible ? backBtn.right : parent.left
                anchors.leftMargin: backBtn.visible ? 8 : 16
                anchors.verticalCenter: parent.verticalCenter
                anchors.right: closeBtn.left
                anchors.rightMargin: 8
                
                Text {
                    text: browsePath === "" ? "Offline Maps" : breadcrumb.length > 0 ? breadcrumb[breadcrumb.length-1].name : "Browse"
                    font.pixelSize: 22
                    font.bold: true
                    color: headerTextColor
                    elide: Text.ElideRight
                    width: parent.width
                }
                
                Text {
                    visible: browsePath !== ""
                    text: {
                        var path = ""
                        for (var i = 0; i < breadcrumb.length - 1; i++) {
                            if (i > 0) path += " > "
                            path += breadcrumb[i].name
                        }
                        return path
                    }
                    font.pixelSize: 12
                    color: headerTextColor
                    opacity: 0.7
                    elide: Text.ElideLeft
                    width: parent.width
                }
            }
            
            Rectangle {
                id: closeBtn
                anchors.right: parent.right
                anchors.rightMargin: 8
                anchors.verticalCenter: parent.verticalCenter
                width: 48; height: 48; radius: 24
                color: downloadBackMA.pressed ? "#00000022" : "transparent"
                
                Text {
                    anchors.centerIn: parent
                    text: "✕"
                    font.pixelSize: 24
                    color: headerTextColor
                }
                
                MouseArea {
                    id: downloadBackMA
                    anchors.fill: parent
                    onClicked: pageStack.pop()
                }
            }
        }
        
        Flickable {
            anchors.top: downloadHeader.bottom
            anchors.left: parent.left
            anchors.right: parent.right
            anchors.bottom: parent.bottom
            contentHeight: downloadCol.height + 40
            clip: true
            
            Column {
                id: downloadCol
                width: parent.width
                spacing: 0
                
                Item { width: 1; height: 16 }
                
                // Status card
                Rectangle {
                    width: parent.width - 32
                    height: statusCol.height + 24
                    anchors.horizontalCenter: parent.horizontalCenter
                    color: cardColor
                    radius: 8
                    
                    Column {
                        id: statusCol
                        anchors {
                            left: parent.left
                            right: parent.right
                            top: parent.top
                            margins: 12
                        }
                        spacing: 8
                        
                        Text {
                            text: "Installed Tiles: " + installedTileCount
                            font.pixelSize: 18
                            font.bold: true
                            color: textColor
                        }
                        
                        Text {
                            text: installedTileCount > 0 ? 
                                "✓ Offline routing available" : 
                                "Download maps for offline routing"
                            font.pixelSize: 14
                            color: installedTileCount > 0 ? "#4CAF50" : textSecondary
                        }
                        
                        Text {
                            text: "Tiles location: MyDocs/Maps.OSM/valhalla/tiles/"
                            font.pixelSize: 12
                            color: textSecondary
                        }
                    }
                }
                
                Item { width: 1; height: 24 }
                
                Text {
                    anchors.left: parent.left
                    anchors.leftMargin: 16
                    text: browsePath === "" ? "Select Region" : "Available"
                    font.pixelSize: 18
                    font.bold: true
                    color: textColor
                }
                
                Item { width: 1; height: 8 }
                
                // Hierarchical regions list
                Repeater {
                    model: browseItems
                    
                    Rectangle {
                        width: downloadCol.width
                        height: 72
                        color: index % 2 === 0 ? cardColor : bgColor
                        
                        Row {
                            anchors.fill: parent
                            anchors.margins: 16
                            anchors.bottomMargin: 20
                            spacing: 12
                            
                            Column {
                                anchors.verticalCenter: parent.verticalCenter
                                width: parent.width - 140
                                
                                Text {
                                    text: modelData ? (modelData.name || "Unknown") : "Loading..."
                                    font.pixelSize: 18
                                    color: textColor
                                    elide: Text.ElideRight
                                    width: parent.width
                                }
                                
                                Text {
                                    text: {
                                        if (!modelData) return ""
                                        var info = []
                                        if (modelData.is_installed) {
                                            info.push("Installed")
                                        } else if (modelData.is_downloadable && modelData.size_mb > 0) {
                                            info.push(modelData.size_mb + " MB")
                                        }
                                        if (modelData.has_subregions) {
                                            info.push("has subregions")
                                        }
                                        // Show download status text
                                        if (modelData && modelData.id) {
                                            var dl = downloadStatus[modelData.id]
                                            if (dl && dl.status) {
                                                if (dl.status === 'error') {
                                                    info = ["Error: " + (dl.error || "Unknown error")]
                                                } else if (dl.status !== 'idle' && dl.status !== 'complete') {
                                                    info = [dl.status + " " + (dl.progress || 0) + "%"]
                                                }
                                            }
                                        }
                                        return info.join(" · ")
                                    }
                                    font.pixelSize: 14
                                    color: {
                                        if (modelData && modelData.id) {
                                            var dl = downloadStatus[modelData.id]
                                            if (dl && dl.status === 'error') return "#F44336"
                                        }
                                        if (modelData && modelData.is_installed) return "#4CAF50"
                                        return textSecondary
                                    }
                                }
                            }
                            
                            Item { width: 1; height: 1 }
                            
                            // Installed indicator
                            Text {
                                text: "OK"
                                font.pixelSize: 16
                                font.bold: true
                                color: "#4CAF50"
                                visible: modelData && modelData.is_installed
                                anchors.verticalCenter: parent.verticalCenter
                            }
                            
                            // Update button (for installed regions - downloads missing geocoder/libpostal)
                            Rectangle {
                                width: modelData && modelData.is_installed ? 50 : 0
                                height: 36
                                radius: 4
                                visible: modelData && modelData.is_installed
                                color: {
                                    if (!modelData || !modelData.id) return "#2196F3"
                                    var dl = downloadStatus[modelData.id]
                                    if (dl && dl.status === 'complete') return "#4CAF50"
                                    if (dl && (dl.progress > 0 || dl.status === 'checking')) return "#FFC107"
                                    return "#2196F3"
                                }
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Text {
                                    anchors.centerIn: parent
                                    text: {
                                        if (!modelData || !modelData.id) return "UPD"
                                        var dl = downloadStatus[modelData.id]
                                        if (dl && dl.status === 'complete') return "✓"
                                        if (dl && dl.progress > 0) return dl.progress + "%"
                                        if (dl && dl.status === 'checking') return "..."
                                        return "UPD"
                                    }
                                    font.pixelSize: 12
                                    color: "white"
                                }
                                
                                MouseArea {
                                    anchors.fill: parent
                                    onClicked: {
                                        if (!modelData || !modelData.id) return
                                        var dl = downloadStatus[modelData.id]
                                        if (!dl || dl.status === 'complete' || dl.status === 'error' || dl.status === 'idle' || !dl.status) {
                                            startUpdate(modelData.id)
                                        }
                                    }
                                }
                            }
                            
                            // Download button (if downloadable and not installed)
                            Rectangle {
                                width: modelData && modelData.is_downloadable && !modelData.is_installed ? 70 : 0
                                height: 36
                                radius: 4
                                visible: modelData && modelData.is_downloadable && !modelData.is_installed
                                color: {
                                    if (!modelData || !modelData.id) return accentColor
                                    var dl = downloadStatus[modelData.id]
                                    if (dl && dl.status === 'complete') return "#4CAF50"
                                    if (dl && dl.progress > 0) return "#FFC107"
                                    return accentColor
                                }
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Text {
                                    anchors.centerIn: parent
                                    text: {
                                        if (!modelData || !modelData.id) return "DL"
                                        var dl = downloadStatus[modelData.id]
                                        if (dl && dl.status === 'complete') return "✓"
                                        if (dl && dl.progress > 0) return dl.progress + "%"
                                        return "DL"
                                    }
                                    font.pixelSize: 14
                                    color: "white"
                                }
                                
                                MouseArea {
                                    anchors.fill: parent
                                    onClicked: {
                                        if (!modelData || !modelData.id) return
                                        var dl = downloadStatus[modelData.id]
                                        if (!dl || dl.status === 'complete' || dl.status === 'error' || dl.status === 'idle') {
                                            startDownload(modelData.id)
                                        }
                                    }
                                }
                            }
                            
                            // Browse button (if has subregions)
                            Rectangle {
                                width: modelData && modelData.has_subregions ? 36 : 0
                                height: 36
                                radius: 4
                                visible: modelData && modelData.has_subregions
                                color: browseMA.pressed ? "#555" : "#666"
                                anchors.verticalCenter: parent.verticalCenter
                                
                                Text {
                                    anchors.centerIn: parent
                                    text: ">"
                                    font.pixelSize: 20
                                    font.bold: true
                                    color: "white"
                                }
                                
                                MouseArea {
                                    id: browseMA
                                    anchors.fill: parent
                                    onClicked: {
                                        if (modelData && modelData.id) {
                                            browseRegions(modelData.id)
                                        }
                                    }
                                }
                            }
                        }
                        
                        // Progress bar at bottom of row
                        Rectangle {
                            anchors.left: parent.left
                            anchors.right: parent.right
                            anchors.bottom: parent.bottom
                            anchors.leftMargin: 16
                            anchors.rightMargin: 16
                            anchors.bottomMargin: 4
                            height: 6
                            color: "#555"
                            radius: 3
                            visible: {
                                if (!modelData || !modelData.id) return false
                                var dl = downloadStatus[modelData.id]
                                if (!dl) return false
                                if (dl.status && dl.status !== 'idle' && dl.status !== 'complete' && dl.status !== 'error') return true
                                return false
                            }
                            
                            Rectangle {
                                width: {
                                    if (!modelData || !modelData.id) return 0
                                    var dl = downloadStatus[modelData.id]
                                    if (dl && dl.progress > 0) {
                                        return parent.width * dl.progress / 100
                                    }
                                    return 0
                                }
                                height: parent.height
                                color: "#4CAF50"
                                radius: 3
                            }
                        }
                        
                        // Make whole row clickable for navigation
                        MouseArea {
                            anchors.fill: parent
                            z: -1
                            onClicked: {
                                if (modelData && modelData.has_subregions) {
                                    browseRegions(modelData.id)
                                } else if (modelData && modelData.is_downloadable && !modelData.is_installed) {
                                    var dl = downloadStatus[modelData.id]
                                    if (!dl || dl.status === 'complete' || dl.status === 'error' || dl.status === 'idle') {
                                        startDownload(modelData.id)
                                    }
                                }
                            }
                        }
                    }
                }
                
                Item { width: 1; height: 24 }
                
                // Alternative: Copy from PC
                Rectangle {
                    width: parent.width - 32
                    height: copyInfoCol.height + 24
                    anchors.horizontalCenter: parent.horizontalCenter
                    color: cardColor
                    radius: 8
                    
                    Column {
                        id: copyInfoCol
                        anchors {
                            left: parent.left
                            right: parent.right
                            top: parent.top
                            margins: 12
                        }
                        spacing: 4
                        
                        Text {
                            text: "Alternative: Copy from PC"
                            font.pixelSize: 16
                            font.bold: true
                            color: textColor
                        }
                        
                        Text {
                            width: parent.width
                            text: "If you have OSMScout Server on your PC, copy the valhalla/tiles folder:"
                            font.pixelSize: 13
                            color: textSecondary
                            wrapMode: Text.WordWrap
                        }
                        
                        Text {
                            width: parent.width
                            text: "scp -r tiles/* user@n9:MyDocs/Maps.OSM/valhalla/tiles/"
                            font.pixelSize: 11
                            font.family: "monospace"
                            color: accentColor
                            wrapMode: Text.WrapAnywhere
                        }
                    }
                }
                
                Item { width: 1; height: 16 }
            }
        }
    }
}
