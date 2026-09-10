# Podcast RSS Feed Specification (2025)

A comprehensive reference for modern podcast RSS feeds, covering RSS 2.0, iTunes namespace, Podcast Index (Podcasting 2.0) namespace, and Atom extensions.

## Namespace Declarations

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
  xmlns:podcast="https://podcastindex.org/namespace/1.0"
  xmlns:atom="http://www.w3.org/2005/Atom"
  xmlns:content="http://purl.org/rss/1.0/modules/content/">
```

| Namespace | URI | Purpose |
|-----------|-----|---------|
| `itunes` | `http://www.itunes.com/dtds/podcast-1.0.dtd` | Apple Podcasts tags |
| `podcast` | `https://podcastindex.org/namespace/1.0` | Podcasting 2.0 tags |
| `atom` | `http://www.w3.org/2005/Atom` | Self-referencing links |
| `content` | `http://purl.org/rss/1.0/modules/content/` | HTML content support |

---

## Channel Elements (Show-Level)

### Required

| Tag | Description | Example |
|-----|-------------|---------|
| `<title>` | Podcast name (max 255 chars, no keywords stuffing) | `<title>My Podcast</title>` |
| `<description>` | Show description (max 4000 bytes) | `<description>About the show...</description>` |
| `<link>` | Website URL | `<link>https://example.com</link>` |
| `<language>` | ISO 639 language code | `<language>en-au</language>` |
| `<atom:link rel="self">` | Canonical feed URL | `<atom:link href="https://example.com/feed.xml" rel="self" type="application/rss+xml"/>` |
| `<itunes:category>` | Apple category (can nest subcategories, multiple allowed) | See below |
| `<itunes:explicit>` | Parental advisory (`true`/`false`) | `<itunes:explicit>false</itunes:explicit>` |
| `<itunes:image>` | Artwork (1400x1400 to 3000x3000 px, JPEG/PNG, RGB) | `<itunes:image href="https://example.com/art.jpg"/>` |

**Category Example:**
```xml
<itunes:category text="Technology">
  <itunes:category text="Tech News"/>
</itunes:category>
<itunes:category text="News"/>
```

### Recommended

| Tag | Description | Example |
|-----|-------------|---------|
| `<itunes:author>` | Creator/host name | `<itunes:author>John Smith</itunes:author>` |
| `<podcast:locked>` | Prevent feed import (`yes`/`no`) | `<podcast:locked>yes</podcast:locked>` |
| `<podcast:guid>` | Permanent UUIDv5 identifier | `<podcast:guid>9b024349-ccf0-5f69-a609-6b82873eab3c</podcast:guid>` |
| `<copyright>` | Copyright notice (no © symbol needed) | `<copyright>2025 My Company</copyright>` |

### Optional

| Tag | Description | Example |
|-----|-------------|---------|
| `<itunes:type>` | `episodic` (default) or `serial` | `<itunes:type>serial</itunes:type>` |
| `<itunes:complete>` | Podcast finished (`yes` only) | `<itunes:complete>yes</itunes:complete>` |
| `<itunes:owner>` | Technical contact | See below |
| `<itunes:new-feed-url>` | Feed migration URL | `<itunes:new-feed-url>https://new.com/feed</itunes:new-feed-url>` |
| `<podcast:funding>` | Donation/support links | `<podcast:funding url="https://patreon.com/show">Support Us</podcast:funding>` |
| `<podcast:txt>` | Verification string (max 4000 chars) | `<podcast:txt purpose="verify">abc123</podcast:txt>` |
| `<podcast:medium>` | Content type | `<podcast:medium>podcast</podcast:medium>` |
| `<podcast:podroll>` | Recommended podcasts | See Podcasting 2.0 section |
| `<podcast:updateFrequency>` | Publishing schedule | See Podcasting 2.0 section |

**Owner Example:**
```xml
<itunes:owner>
  <itunes:name>John Smith</itunes:name>
  <itunes:email>john@example.com</itunes:email>
</itunes:owner>
```

---

## Item Elements (Episode-Level)

### Required

| Tag | Description | Example |
|-----|-------------|---------|
| `<title>` | Episode title (no show name, no ep numbers) | `<title>Interview with Jane</title>` |
| `<enclosure>` | Media file (url, length in bytes, type) | See below |
| `<guid>` | Unique identifier (never change) | `<guid isPermaLink="false">ep-001-abc123</guid>` |

**Enclosure Example:**
```xml
<enclosure 
  url="https://example.com/ep001.mp3" 
  length="45000000" 
  type="audio/mpeg"/>
```

Supported types: `audio/mpeg`, `audio/m4a`, `audio/x-m4a`, `audio/aac`, `audio/ogg`, `audio/opus`, `video/mp4`, `video/m4v`, `video/quicktime`

### Recommended

| Tag | Description | Example |
|-----|-------------|---------|
| `<pubDate>` | Release date (RFC 2822 format) | `<pubDate>Wed, 01 Jan 2025 10:00:00 +1100</pubDate>` |
| `<description>` | Episode description (max 4000 bytes) | `<description>In this episode...</description>` |
| `<link>` | Episode webpage | `<link>https://example.com/ep001</link>` |
| `<itunes:duration>` | Duration in seconds | `<itunes:duration>3600</itunes:duration>` |
| `<itunes:image>` | Episode artwork | `<itunes:image href="https://example.com/ep001.jpg"/>` |
| `<itunes:explicit>` | Episode parental advisory | `<itunes:explicit>false</itunes:explicit>` |
| `<podcast:transcript>` | Transcript file(s) | See Podcasting 2.0 section |

### Optional

| Tag | Description | Example |
|-----|-------------|---------|
| `<itunes:episode>` | Episode number (non-zero integer) | `<itunes:episode>42</itunes:episode>` |
| `<itunes:season>` | Season number | `<itunes:season>2</itunes:season>` |
| `<itunes:episodeType>` | `full`, `trailer`, or `bonus` | `<itunes:episodeType>full</itunes:episodeType>` |
| `<itunes:block>` | Hide episode (`yes` only) | `<itunes:block>yes</itunes:block>` |
| `<itunes:title>` | Title override (display only) | `<itunes:title>Short Title</itunes:title>` |
| `<content:encoded>` | Rich HTML description | `<content:encoded><![CDATA[<p>HTML content</p>]]></content:encoded>` |

---

## Podcasting 2.0 Tags (podcast: namespace)

All formalized tags from the Podcast Index namespace (Phase 1-8):

### Channel-Level

| Tag | Description |
|-----|-------------|
| `<podcast:locked>` | Import protection |
| `<podcast:guid>` | Permanent podcast identifier |
| `<podcast:funding>` | Donation/support links (multiple allowed) |
| `<podcast:txt>` | Verification/ownership proof |
| `<podcast:medium>` | Content type: `podcast`, `music`, `video`, `film`, `audiobook`, `newsletter`, `blog`, `publisher` |
| `<podcast:trailer>` | Promotional trailer |
| `<podcast:license>` | Content license |
| `<podcast:podroll>` | Recommended podcasts |
| `<podcast:updateFrequency>` | Publishing schedule |
| `<podcast:block>` | Platform-specific blocking |
| `<podcast:value>` | Value-for-value payments (Bitcoin Lightning) |
| `<podcast:publisher>` | Publisher information |

### Item-Level

| Tag | Description |
|-----|-------------|
| `<podcast:transcript>` | Transcript/captions files |
| `<podcast:chapters>` | Chapter markers (JSON file) |
| `<podcast:soundbite>` | Audio clips for previews/sharing |
| `<podcast:person>` | Hosts, guests, credits |
| `<podcast:location>` | Geographic location |
| `<podcast:season>` | Season metadata |
| `<podcast:episode>` | Episode metadata |
| `<podcast:alternateEnclosure>` | Alternative media files |
| `<podcast:source>` | Media sources within alternateEnclosure |
| `<podcast:integrity>` | File verification hash |
| `<podcast:socialInteract>` | Comments/discussion platform |
| `<podcast:contentLink>` | Additional content links |
| `<podcast:value>` | Episode-level payments |
| `<podcast:valueTimeSplit>` | Time-based payment splits |
| `<podcast:chat>` | Live chat integration |
| `<podcast:liveItem>` | Live streaming episodes |
| `<podcast:remoteItem>` | Reference external episodes |

---

## Detailed Podcasting 2.0 Examples

### Transcript
```xml
<podcast:transcript 
  url="https://example.com/ep001/transcript.vtt" 
  type="text/vtt"/>
<podcast:transcript 
  url="https://example.com/ep001/transcript.json" 
  type="application/json" 
  language="en"/>
<podcast:transcript 
  url="https://example.com/ep001/captions.srt" 
  type="application/x-subrip" 
  rel="captions"/>
```

Supported types: `text/plain`, `text/html`, `text/vtt`, `application/json`, `application/x-subrip`

### Chapters
```xml
<podcast:chapters 
  url="https://example.com/ep001/chapters.json" 
  type="application/json+chapters"/>
```

Chapters JSON format:
```json
{
  "version": "1.2.0",
  "chapters": [
    {
      "startTime": 0,
      "title": "Introduction",
      "img": "https://example.com/ch1.jpg",
      "url": "https://example.com/intro"
    },
    {
      "startTime": 180,
      "title": "Main Topic",
      "toc": true
    }
  ]
}
```

### Soundbite
```xml
<podcast:soundbite startTime="73.0" duration="60.0"/>
<podcast:soundbite startTime="1234.5" duration="42.25">
  Highlight Title
</podcast:soundbite>
```

### Person
```xml
<podcast:person 
  href="https://example.com/jane" 
  img="https://example.com/jane.jpg">Jane Smith</podcast:person>
<podcast:person 
  role="guest" 
  href="https://twitter.com/guest" 
  img="https://example.com/guest.jpg">Guest Name</podcast:person>
<podcast:person 
  group="visuals" 
  role="cover art designer">Artist Name</podcast:person>
```

Roles from [Podcast Taxonomy Project](https://github.com/Podcastindex-org/podcast-namespace/blob/main/taxonomy.json)

### Location
```xml
<podcast:location 
  rel="subject" 
  geo="geo:-33.8688,151.2093" 
  osm="R5750005" 
  country="AU">Sydney</podcast:location>
<podcast:location 
  rel="creator" 
  geo="geo:-33.7369,151.1053" 
  country="AU">Mount Colah</podcast:location>
```

### Alternate Enclosure
```xml
<podcast:alternateEnclosure 
  type="audio/mpeg" 
  length="43200000" 
  bitrate="128000" 
  default="true" 
  title="Standard MP3">
  <podcast:source uri="https://example.com/ep.mp3"/>
  <podcast:source uri="ipfs://Qm..."/>
</podcast:alternateEnclosure>

<podcast:alternateEnclosure 
  type="audio/opus" 
  length="32400000" 
  bitrate="96000" 
  title="High Quality Opus">
  <podcast:source uri="https://example.com/ep.opus"/>
</podcast:alternateEnclosure>

<podcast:alternateEnclosure 
  type="video/mp4" 
  length="500000000" 
  height="1080" 
  title="Video Version">
  <podcast:source uri="https://example.com/ep.mp4"/>
  <podcast:integrity type="sri" value="sha384-..."/>
</podcast:alternateEnclosure>
```

### Live Item
```xml
<podcast:liveItem 
  status="live" 
  start="2025-01-01T10:00:00+11:00" 
  end="2025-01-01T12:00:00+11:00">
  <title>Live Episode</title>
  <guid>live-001</guid>
  <enclosure url="https://example.com/live.mp3" type="audio/mpeg" length="0"/>
  <podcast:contentLink href="https://youtube.com/live/xyz">YouTube</podcast:contentLink>
  <podcast:contentLink href="https://twitch.tv/show">Twitch</podcast:contentLink>
</podcast:liveItem>
```

Status values: `pending`, `live`, `ended`

### Value (Bitcoin Lightning)
```xml
<podcast:value type="lightning" method="keysend" suggested="0.00000005000">
  <podcast:valueRecipient 
    name="Host" 
    type="node" 
    address="03..." 
    split="90"/>
  <podcast:valueRecipient 
    name="Producer" 
    type="node" 
    address="02..." 
    split="10"/>
</podcast:value>
```

### Podroll (Recommendations)
```xml
<podcast:podroll>
  <podcast:remoteItem 
    feedGuid="9b024349-ccf0-5f69-a609-6b82873eab3c" 
    feedUrl="https://other.com/feed.xml" 
    title="Recommended Show"/>
</podcast:podroll>
```

### Update Frequency
```xml
<podcast:updateFrequency complete="true" rrule="FREQ=WEEKLY;BYDAY=MO">
  Weekly on Mondays
</podcast:updateFrequency>
```

### Social Interact
```xml
<podcast:socialInteract 
  uri="https://mastodon.social/@podcast/123" 
  protocol="activitypub" 
  accountId="@podcast@mastodon.social"/>
```

### Block (Platform-Specific)
```xml
<podcast:block id="google">yes</podcast:block>
<podcast:block id="amazon">yes</podcast:block>
```

---

## Complete Feed Example

```xml
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"
  xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd"
  xmlns:podcast="https://podcastindex.org/namespace/1.0"
  xmlns:atom="http://www.w3.org/2005/Atom"
  xmlns:content="http://purl.org/rss/1.0/modules/content/">
  <channel>
    <title>Example Podcast</title>
    <description>A podcast about interesting topics.</description>
    <link>https://example.com</link>
    <language>en-au</language>
    <atom:link href="https://example.com/feed.xml" rel="self" type="application/rss+xml"/>
    
    <itunes:author>John Smith</itunes:author>
    <itunes:category text="Technology"/>
    <itunes:explicit>false</itunes:explicit>
    <itunes:image href="https://example.com/artwork.jpg"/>
    <itunes:type>episodic</itunes:type>
    
    <podcast:locked>no</podcast:locked>
    <podcast:guid>a1b2c3d4-e5f6-7890-abcd-ef1234567890</podcast:guid>
    <podcast:funding url="https://ko-fi.com/example">Buy me a coffee</podcast:funding>
    <podcast:person href="https://example.com/john" img="https://example.com/john.jpg">John Smith</podcast:person>
    <podcast:medium>podcast</podcast:medium>
    
    <item>
      <title>Episode 1: Getting Started</title>
      <enclosure url="https://example.com/ep001.mp3" length="45000000" type="audio/mpeg"/>
      <guid isPermaLink="false">ep001-unique-id</guid>
      <pubDate>Wed, 01 Jan 2025 10:00:00 +1100</pubDate>
      <description>In this episode, we explore the basics.</description>
      <link>https://example.com/episodes/001</link>
      
      <itunes:duration>2700</itunes:duration>
      <itunes:episode>1</itunes:episode>
      <itunes:season>1</itunes:season>
      <itunes:episodeType>full</itunes:episodeType>
      <itunes:explicit>false</itunes:explicit>
      <itunes:image href="https://example.com/ep001.jpg"/>
      
      <podcast:transcript url="https://example.com/ep001/transcript.vtt" type="text/vtt"/>
      <podcast:chapters url="https://example.com/ep001/chapters.json" type="application/json+chapters"/>
      <podcast:soundbite startTime="120" duration="30">Key Moment</podcast:soundbite>
      <podcast:person role="host" img="https://example.com/john.jpg">John Smith</podcast:person>
      <podcast:person role="guest" img="https://example.com/guest.jpg">Guest Name</podcast:person>
      
      <podcast:alternateEnclosure type="audio/opus" length="30000000" bitrate="96000" title="High Quality">
        <podcast:source uri="https://example.com/ep001.opus"/>
      </podcast:alternateEnclosure>
    </item>
  </channel>
</rss>
```

---

## Key Technical Requirements

### Encoding
- UTF-8 encoding required
- Use `<![CDATA[...]]>` for HTML content
- Entity escaping: `&apos;`, `&quot;`, `&amp;`, `&lt;`, `&gt;`
- Avoid HTML entities like `&rsquo;`

### Dates
RFC 2822 format: `Day, DD Mon YYYY HH:MM:SS +ZZZZ`
```
Wed, 01 Jan 2025 10:00:00 +1100
```

### URLs
- HTTPS required for Podcasting 2.0 tags
- ASCII characters only in filenames
- Must support HTTP HEAD requests and byte-range requests

### Images
- Minimum: 1400×1400px
- Maximum: 3000×3000px
- Format: JPEG or PNG
- Color: RGB
- Resolution: 72 DPI

### GUID Generation (podcast:guid)
UUIDv5 from feed URL using namespace `ead4c236-bf58-58c6-a2c6-a6b28d128cb6`:
```
Feed: https://example.com/feed.xml
Strip: example.com/feed.xml
Result: UUIDv5(namespace, "example.com/feed.xml")
```

---

## Platform Support Notes

| Feature | Apple | Spotify | Google | Podcast Index Apps |
|---------|-------|---------|--------|-------------------|
| itunes:* tags | ✅ | ✅ | ✅ | ✅ |
| podcast:transcript | ✅ | ✅ | ✅ | ✅ |
| podcast:chapters | ❌ | ❌ | ❌ | ✅ |
| podcast:person | ❌ | ❌ | ❌ | ✅ |
| podcast:value | ❌ | ❌ | ❌ | ✅ |
| podcast:liveItem | ❌ | ❌ | ❌ | ✅ |
| podcast:alternateEnclosure | ❌ | ❌ | ❌ | ✅ |

---

## Resources

- [RSS 2.0 Specification](https://www.rssboard.org/rss-specification)
- [Apple Podcasts Requirements](https://podcasters.apple.com/support/823-podcast-requirements)
- [Podcast Index Namespace](https://github.com/Podcastindex-org/podcast-namespace/blob/main/docs/1.0.md)
- [PSP-1 Standard](https://github.com/Podcast-Standards-Project/PSP-1-Podcast-RSS-Specification)
- [Podcast Namespace Examples](https://podcastnamespace.org/)
- [Podcasting 2.0 Apps](https://podcastindex.org/apps)
