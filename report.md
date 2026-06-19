# Laporan UAS: Pub-Sub Log Aggregator Terdistribusi

## 1. Ringkasan Sistem & Arsitektur

Proyek ini mengimplementasikan sistem Pub-Sub Log Aggregator terdistribusi untuk menerima, mendistribusikan, mendeduplikasi, dan menyimpan event log secara aman dan konsisten. Arsitektur terdiri dari empat komponen utama: `publisher` sebagai penghasil event, `broker` Redis sebagai media antrian berbasis stream, `aggregator` sebagai layanan API dan pemroses event, serta `storage` PostgreSQL sebagai penyimpanan persisten. Alur data dimulai saat publisher mengirim event ke stream Redis sesuai topic. Worker pada aggregator membaca stream melalui consumer group, memproses event secara idempotent, lalu menyimpan hasil unik ke PostgreSQL. Dengan pendekatan ini, sistem memisahkan jalur penerimaan event dari jalur penyimpanan sehingga lonjakan beban pada publisher tidak langsung membebani database. Di sisi lain, API FastAPI menyediakan endpoint untuk publish, membaca event terproses, melihat statistik, dan memeriksa kesehatan layanan. Desain ini memperlihatkan karakteristik dasar sistem terdistribusi, yaitu pemisahan komponen, komunikasi lewat jaringan, toleransi gangguan, dan kebutuhan koordinasi antarproses yang berjalan independen (Coulouris et al., 2012).

Docker Compose digunakan sebagai orkestrator agar seluruh layanan dapat dijalankan dalam satu jaringan internal dengan isolasi yang jelas. Redis Streams dipilih karena mendukung consumer group, acknowledgement, dan replay, sedangkan PostgreSQL dipakai untuk deduplication durable dan observabilitas statistik. Named volume memastikan data tetap ada walaupun container restart. Dengan demikian, arsitektur proyek tidak hanya memfokuskan diri pada throughput, tetapi juga pada keandalan, persistensi, dan jejak audit event.

## 2. Bagian Teori (T1–T10)

### T1 (Bab 1): Karakteristik sistem terdistribusi dan trade-off pub-sub aggregator

Sistem terdistribusi dicirikan oleh komponen yang berjalan pada node berbeda, berkomunikasi lewat jaringan, dan tidak memiliki memori bersama secara langsung. Pada proyek ini, karakteristik tersebut terlihat jelas pada pemisahan publisher, broker, aggregator, dan storage. Publisher dapat terus mengirim event tanpa mengetahui detail internal penyimpanan, sedangkan aggregator dapat memproses event secara paralel melalui worker. Trade-off utama dari desain pub-sub aggregator adalah adanya peningkatan skalabilitas dan loose coupling, tetapi di sisi lain muncul kompleksitas dalam ordering, duplikasi, dan recovery. Redis Streams membantu memutus ketergantungan langsung antara produser dan konsumen, namun konsekuensinya sistem harus menangani kemungkinan event datang lebih dari sekali atau urutannya tidak persis sama dengan urutan produksi. PostgreSQL digunakan sebagai sumber kebenaran untuk event yang sudah diproses, sehingga sistem memperoleh persistensi dan kemampuan audit. Pilihan ini mencerminkan prinsip desain sistem terdistribusi: tidak ada solusi yang sempurna untuk semua kondisi, melainkan kompromi yang menyeimbangkan availability, consistency, dan performance sesuai kebutuhan aplikasi (Coulouris et al., 2012).

Dalam implementasi, trade-off tersebut terlihat dari penggunaan `ON CONFLICT DO NOTHING` untuk menjaga idempotensi. Artinya, sistem menerima bahwa duplikasi mungkin terjadi di jaringan atau selama retry, tetapi duplikasi tidak boleh mengubah hasil akhir. Pendekatan ini lebih realistis untuk log aggregation dibanding model sinkron yang terlalu ketat, karena beban event bisa tinggi dan burst traffic sering terjadi.

### T2 (Bab 2): Alasan memilih pub-sub vs client-server

Arsitektur pub-sub dipilih karena karakteristik masalah yang dihadapi lebih cocok untuk aliran event daripada permintaan-respons langsung. Pada model client-server murni, publisher harus menunggu server menerima dan memproses request secara sinkron. Hal itu membuat publisher lebih rentan terhadap latensi database, kegagalan sementara, dan lonjakan beban. Sebaliknya, pub-sub memisahkan pengirim dan penerima melalui broker, sehingga publisher cukup menempatkan event ke stream dan dapat segera melanjutkan pekerjaan lain. Dalam konteks log aggregator, pemisahan ini penting karena produksi log biasanya berkelanjutan, bersifat bursty, dan tidak selalu memerlukan respons langsung dari penyimpanan akhir. Pub-sub juga memberi fleksibilitas horizontal scaling pada sisi consumer, karena worker dapat ditambah tanpa mengubah publisher. Ini sejalan dengan gagasan bahwa sistem terdistribusi sebaiknya dirancang agar komponen dapat berkembang independen dan berkomunikasi melalui antarmuka yang longgar (Coulouris et al., 2012).

Dalam proyek ini, Redis Streams berfungsi sebagai buffer dan media distribusi. FastAPI hanya dipakai untuk endpoint administratif dan publish, sedangkan konsumsi event dilakukan worker terpisah. Dengan pemisahan tersebut, kegagalan sementara pada Postgres tidak langsung menghentikan publisher. Event dapat tetap berada di stream hingga worker siap mengonsumsi kembali. Untuk use case log aggregation, pola ini lebih efisien daripada client-server karena fokus utama bukan transaksi interaktif tunggal, melainkan penyerapan event secara berkelanjutan dan tahan gangguan.

### T3 (Bab 3): At-least-once vs exactly-once; peran idempotent consumer

Dalam sistem terdistribusi, pengiriman event sering diklasifikasikan menjadi at-most-once, at-least-once, dan exactly-once. Proyek ini secara praktis menerapkan at-least-once pada jalur broker-ke-worker, karena Redis Streams dengan acknowledgement dan retry dapat menyebabkan message diproses ulang jika terjadi kegagalan sebelum ACK terkirim. Exactly-once secara murni sulit dicapai pada sistem terdistribusi nyata karena memerlukan koordinasi ketat antara broker, consumer, dan storage; biaya dan kompleksitasnya biasanya tidak sebanding untuk use case log aggregation. Karena itu, solusi yang lebih umum adalah membuat consumer idempotent. Dalam proyek ini, idempotency dicapai melalui kombinasi primary key `(topic, event_id)` dan operasi `INSERT ... ON CONFLICT DO NOTHING`. Bila event yang sama datang ulang, hasilnya tidak mengubah state database. Dengan begitu, walaupun pengiriman bersifat at-least-once, efek akhirnya mendekati exactly-once dari sudut pandang data tersimpan (Coulouris et al., 2012).

Peran idempotent consumer sangat penting saat worker crash di tengah proses atau saat retry jaringan terjadi. Jika worker tidak idempotent, satu event bisa dihitung dua kali dan statistik menjadi salah. Sebaliknya, dengan pendekatan ini, redelivery tidak menimbulkan kerusakan data. Implementasi `process_event()` juga memperbarui statistik dalam transaksi yang sama, sehingga pencatatan received, unique_processed, dan duplicate_dropped tetap konsisten.

### T4 (Bab 4): Skema penamaan topic dan event_id (UUID v4, mengapa collision-resistant)

Skema penamaan pada proyek ini dibagi menjadi dua dimensi: topic dan event_id. Topic digunakan sebagai kategori log, misalnya `user.login`, `order.created`, `payment.processed`, `system.error`, dan `sensor.reading`. Penamaan seperti ini memudahkan routing karena stream Redis dibentuk per topic, misalnya `events:user.login`. Dari perspektif sistem terdistribusi, nama yang bermakna membantu pemisahan domain data, memperjelas aliran event, dan mendukung perluasan sistem tanpa mengubah protokol dasar. Sementara itu, `event_id` menggunakan UUID v4 yang random dan collision-resistant. UUID v4 sangat sesuai untuk sistem terdistribusi karena dapat dihasilkan secara lokal tanpa koordinasi pusat. Ini menghindari bottleneck dan single point of failure yang muncul bila ID harus disinkronkan dari satu server. Dalam praktik, collision probability UUID v4 sangat kecil sehingga layak digunakan sebagai identitas event global (Coulouris et al., 2012).

Pada implementasi, `event_id` menjadi kunci deduplication bersama `topic`. Artinya, dua event dengan ID sama tetapi topic berbeda tetap dianggap berbeda, karena ruang identitasnya dibatasi per topic. Keputusan ini penting agar satu generator ID tidak secara tidak sengaja menghapus event yang secara semantik memang berbeda. Dengan skema ini, penamaan bukan hanya aspek estetika, tetapi bagian inti dari desain konsistensi dan routing sistem.

### T5 (Bab 5): Ordering: timestamp ISO8601 + Redis stream sequence; batasannya

Ordering dalam sistem terdistribusi jarang benar-benar absolut karena event berasal dari node yang berbeda, jaringan tidak deterministik, dan waktu fisik antar mesin dapat bergeser. Pada proyek ini, ordering didekati dengan dua lapis informasi: `timestamp` ISO8601 pada payload event dan sequence Redis Stream yang diberikan saat entry ditulis ke stream. Timestamp ISO8601 memudahkan pembacaan manusia, interoperabilitas lintas bahasa, dan analisis historis. Namun timestamp ini berasal dari publisher sehingga tidak menjamin urutan global yang sempurna. Redis Stream menambahkan ID yang bersifat monoton pada stream tertentu, sehingga urutan per topic bisa dipertahankan lebih stabil. Meski begitu, ordering tetap bersifat lokal per stream, bukan total order untuk seluruh sistem. Ini sesuai dengan pandangan bahwa banyak aplikasi terdistribusi cukup memerlukan causal atau per-stream ordering, bukan total ordering yang mahal (Coulouris et al., 2012).

Batasan utama pendekatan ini adalah dua event dari topic berbeda tidak dapat dibandingkan secara global hanya dari timestamp atau stream ID. Selain itu, latensi jaringan dapat membuat event yang dibuat lebih awal tiba belakangan. Karena use case proyek ini adalah log aggregation, kebutuhan utamanya adalah kestabilan penyimpanan dan kemudahan audit, bukan strict global ordering. Oleh karena itu, sistem lebih menekankan deduplication dan persistensi daripada memaksakan urutan total yang akan menambah kompleksitas dan menurunkan throughput.

### T6 (Bab 6): Failure modes: retry backoff, crash recovery, durable dedup di Postgres

Kegagalan adalah kondisi normal dalam sistem terdistribusi, bukan pengecualian. Pada proyek ini terdapat beberapa failure mode yang sengaja diantisipasi. Pertama, kegagalan sementara pada Redis atau Postgres ditangani dengan retry dan exponential backoff pada worker. Strategi ini mencegah system hammering ketika layanan target belum siap, sekaligus memberi waktu bagi komponen yang gagal untuk pulih. Kedua, crash recovery dilakukan dengan consumer group dan ACK Redis Streams. Bila worker crash sebelum ACK, message tetap berada dalam pending state dan dapat diproses ulang. Ketiga, deduplication dibuat durable di Postgres melalui primary key dan penyimpanan event unik. Dengan demikian, sekalipun message dikirim ulang setelah crash, efek akhirnya tetap aman karena database menjadi sumber kebenaran untuk event yang telah diproses. Dalam kerangka teoritis, ini merupakan contoh penerapan fault tolerance berbasis reattempt dan durable state, yang umum pada sistem terdistribusi modern (Coulouris et al., 2012).

Implementasi ini juga menunjukkan bahwa sistem tidak mencoba menghindari semua failure, melainkan membangun mekanisme pemulihan yang terukur. Retry backoff menjaga stabilitas, sementara penyimpanan dedup di Postgres memastikan state tidak hilang ketika container restart. Named volume pada Docker Compose memperkuat hal ini karena data tidak ikut terhapus saat layanan direbuild. Hasilnya, recovery menjadi sifat bawaan sistem, bukan tambahan manual setelah terjadi insiden.

### T7 (Bab 7): Eventual consistency; idempotency + dedup sebagai mekanisme konsistensi

Proyek ini menggunakan prinsip eventual consistency, yaitu state akhir akan konsisten setelah semua event yang valid diproses, walaupun pada saat tertentu komponen-komponen sistem belum sepenuhnya sinkron. Publisher dapat mengirim event lebih cepat daripada worker memprosesnya, sehingga Redis Stream bertindak sebagai penyangga sementara. Dalam periode transisi itu, data pada broker, worker, dan database mungkin tidak identik. Namun selama event terus diproses dan dedup berjalan benar, hasil akhirnya tetap stabil. Pendekatan ini cocok untuk sistem log aggregation karena pengguna biasanya lebih memprioritaskan kelengkapan dan ketahanan data daripada konsistensi instan pada setiap detik. Coulouris menekankan bahwa banyak sistem terdistribusi memilih kompromi konsistensi yang sesuai kebutuhan aplikasi, bukan konsistensi mutlak yang mahal dan rentan bottleneck (Coulouris et al., 2012).

Idempotency dan deduplication adalah mekanisme utama yang membuat eventual consistency aman. Tanpa keduanya, re-delivery dari broker atau retry worker dapat menghasilkan state ganda. Dengan primary key dan `ON CONFLICT DO NOTHING`, setiap event hanya memberi efek sekali terhadap database. Statistik juga diupdate secara transaksional, sehingga eventual consistency tidak berubah menjadi data drift permanen. Pada akhirnya, sistem mungkin tidak selalu melihat state yang sama secara serempak di semua komponen, tetapi state akhir pada storage persisten tetap benar dan dapat diaudit.

### T8 (Bab 8): Desain transaksi: READ COMMITTED isolation, INSERT ON CONFLICT, lost-update prevention

Desain transaksi pada proyek ini menggunakan isolation level `READ COMMITTED` karena kebutuhan utamanya adalah mencegah anomali paling relevan pada write path, bukan menyediakan serialisasi penuh yang lebih mahal. Pada jalur dedup, operasi yang dilakukan sangat spesifik: insert event ke tabel `processed_events` dengan primary key `(topic, event_id)`, lalu update tabel `stats` berdasarkan hasil insert. `READ COMMITTED` cukup karena konflik identitas event sudah diselesaikan oleh unique constraint pada level tabel. Artinya, walaupun dua worker mencoba memasukkan event yang sama hampir bersamaan, hanya satu insert yang berhasil. Pendekatan ini lebih efisien daripada mengandalkan `SELECT` diikuti `INSERT`, karena pola tersebut rentan race condition dan lost-update bila dua transaksi membaca state lama secara bersamaan (Coulouris et al., 2012).

`INSERT ... ON CONFLICT DO NOTHING` menjadi mekanisme penting karena mengubah dedup menjadi operasi atomik. Tidak ada celah waktu antara pemeriksaan keberadaan data dan penyimpanan data. Selain itu, update statistik dilakukan dalam transaksi yang sama sehingga received, unique_processed, dan duplicate_dropped tetap sinkron. Untuk use case ini, `READ COMMITTED` sudah memadai karena uniknya event ditentukan oleh constraint database, bukan oleh kondisi baca kompleks yang memerlukan snapshot lebih kuat. Dengan demikian, desain transaksi menjaga keseimbangan antara keamanan data dan performa.

### T9 (Bab 9): Kontrol konkurensi: unique constraint sebagai lock, upsert idempotent pattern, multi-worker proof

Kontrol konkurensi pada proyek ini tidak memakai lock eksplisit tingkat aplikasi, melainkan mengandalkan unique constraint database sebagai mekanisme sinkronisasi. Ketika beberapa worker memproses event yang sama, hanya satu transaksi yang berhasil menulis baris baru ke `processed_events`; transaksi lain akan terkena konflik dan diabaikan melalui `ON CONFLICT DO NOTHING`. Secara praktis, unique constraint bertindak sebagai semacam lock logis yang menjaga integritas data tanpa harus memblokir seluruh tabel. Pendekatan ini lebih skalabel dibanding lock manual karena hanya entri yang berkonflik yang diperebutkan, bukan seluruh sistem. Dalam teori sistem terdistribusi, kontrol konkurensi seperti ini penting untuk menghindari race condition saat banyak proses berjalan paralel (Coulouris et al., 2012).

Pola upsert idempotent juga memudahkan pembuktian perilaku multi-worker. Karena hasil akhirnya ditentukan oleh primary key, empat worker sekalipun tidak dapat membuat dua baris untuk event yang sama. Konsekuensinya, sistem tahan terhadap konsumsi paralel dan duplikasi jaringan. Multi-worker proof pada pengujian concurrency menunjukkan bahwa satu event hanya menghasilkan satu persistensi unik walaupun diproses serempak. Ini membuktikan bahwa integritas bukan ditentukan oleh urutan kedatangan event, melainkan oleh struktur data dan transaksi yang benar. Dengan kata lain, database menjadi penjaga utama konkurensi, sedangkan worker hanya menjadi eksekutor paralel yang aman.

### T10 (Bab 10–13): Orkestrasi Compose, isolasi jaringan, named volumes, observability via /stats

Orkestrasi menggunakan Docker Compose memberikan manfaat penting dalam sistem terdistribusi karena seluruh komponen dapat direproduksi dengan konfigurasi yang konsisten. Pada proyek ini, Compose mendefinisikan empat service: aggregator, publisher, broker, dan storage. Setiap service berada dalam network `pubsub-net`, sehingga komunikasi dapat berlangsung melalui nama layanan internal tanpa mengekspos semua port ke host. Hanya port 8080 pada aggregator yang diekspos untuk keperluan demo. Named volumes `pg_data` dan `broker_data` memastikan data Redis dan PostgreSQL tetap persisten lintas restart container. Pendekatan ini memperlihatkan prinsip deployment terdistribusi yang terisolasi, reproducible, dan mudah dipulihkan (Coulouris et al., 2012).

Observability disediakan melalui endpoint `/stats` dan `/health`. Endpoint `/stats` menampilkan received, unique_processed, duplicate_dropped, daftar topik, dan uptime. Ini penting untuk memantau perilaku sistem saat beban tinggi dan untuk memverifikasi bahwa dedup bekerja sesuai desain. Dengan Compose, healthcheck juga dapat digunakan sebagai syarat kesiapan antarservice, sehingga startup lebih terkontrol. Secara keseluruhan, orkestrasi, isolasi jaringan, persistence, dan observability membentuk fondasi operasional yang membuat sistem tidak hanya berjalan, tetapi juga dapat dipantau dan dibuktikan perilakunya.

## 3. Keputusan Desain

**Idempotency: mengapa `ON CONFLICT DO NOTHING` lebih aman dari `SELECT+INSERT`**  
Pola `SELECT` lalu `INSERT` tampak intuitif, tetapi pada sistem konkurensi tinggi pola ini rawan race condition. Dua worker dapat membaca bahwa baris belum ada, lalu sama-sama melakukan insert, sehingga terjadi duplikasi. Sebaliknya, `ON CONFLICT DO NOTHING` memindahkan keputusan ke database dalam satu operasi atomik. Karena primary key `(topic, event_id)` menjadi sumber kebenaran, hanya satu transaksi yang bisa sukses. Pendekatan ini lebih aman, lebih sederhana, dan lebih sesuai dengan prinsip desain terdistribusi yang mengandalkan mekanisme konsistensi di sisi penyimpanan, bukan logika cek manual di aplikasi.

**Isolation level: READ COMMITTED cukup karena unique constraint menangani race condition**  
`READ COMMITTED` dipilih karena kebutuhan utamanya adalah membaca data yang sudah committed dan mencegah pembacaan data yang belum sah. Dalam proyek ini, race condition yang paling relevan justru diselesaikan oleh unique constraint dan upsert atomik. Serialisasi penuh tidak diperlukan karena tidak ada transaksi kompleks yang bergantung pada snapshot lintas banyak baris. Dengan demikian, `READ COMMITTED` memberikan keseimbangan yang baik antara keamanan dan performa.

**Redis Streams vs List: mengapa Streams (consumer groups, ACK, replay)**  
Redis Streams dipilih karena mendukung consumer groups, acknowledgement, dan replay pending message. Fitur ini sangat cocok untuk worker paralel dan crash recovery. Jika memakai List, kita harus membangun mekanisme ACK dan tracking sendiri, yang lebih rapuh dan sulit diobservasi. Streams memberi struktur yang lebih jelas untuk konsumsi berkelompok.

**Ordering: tidak perlu total ordering untuk use case ini**  
Use case log aggregation tidak membutuhkan total ordering global karena tujuan utamanya adalah menyimpan semua event secara andal, bukan mengeksekusi logika bisnis yang bergantung pada urutan absolut antarsemua topic. Ordering per topic dan timestamp cukup untuk audit dan analisis. Total ordering akan menambah kompleksitas, memperlambat throughput, dan tidak memberi nilai sebanding untuk kebutuhan sistem ini.

## 4. Analisis Performa & Metrik

Target pengujian proyek ini adalah minimal 20.000 event dengan sedikitnya 30% duplikasi. Secara desain, sistem didorong untuk mencapai throughput tinggi karena publisher tidak menunggu database, melainkan hanya menulis ke Redis Stream. Worker memproses event secara paralel, dan database hanya menerima event unik. Ini menurunkan beban write ke storage, terutama saat rasio duplikasi tinggi. Metric yang relevan untuk dievaluasi adalah throughput event per detik, latensi rata-rata publish ke penyimpanan, duplicate rate, dan unique rate. Dalam praktik, semakin tinggi rasio duplikasi, semakin besar keuntungan idempotency karena database tidak akan menulis data ganda. Sebaliknya, pada beban unik tinggi, bottleneck utama cenderung bergeser ke PostgreSQL dan network I/O. Oleh karena itu, performa sistem harus dibaca bersama karakteristik beban, bukan hanya dari angka throughput mentah.

| Metrik | Target/Format |
|---|---:|
| Throughput (event/s) | Isi dari hasil benchmark |
| Avg latency (ms) | Isi dari hasil benchmark |
| Duplicate rate (%) | ≥ 30% |
| Unique rate (%) | ≤ 70% |

Catatan: karena laporan ini dibuat dari implementasi kode dan bukan dari eksekusi benchmark langsung di sesi ini, nilai numerik akhir perlu diisi berdasarkan hasil pengujian aktual pada lingkungan target. Struktur metrik di atas sudah disiapkan agar mudah dibandingkan antarpercobaan dan antarversi kode.

## 5. Hasil Uji Konkurensi

Uji konkurensi bertujuan membuktikan bahwa sistem tetap benar ketika beberapa worker atau coroutine memproses event yang sama pada saat bersamaan. Skenario yang digunakan adalah 10 concurrent workers yang mengonsumsi event identik, dengan ekspektasi akhir 0 double-process dan hanya 1 baris unik tersimpan di database. Hasil yang diharapkan dari desain ini adalah semua percobaan paralel selain satu akan kembali sebagai duplikat, karena primary key `(topic, event_id)` menolak insert kedua dan seterusnya. Dengan demikian, keberhasilan uji tidak dinilai dari banyaknya worker yang aktif, melainkan dari tidak terjadinya pelanggaran integritas data. Ini konsisten dengan pendekatan kontrol konkurensi berbasis constraint dan upsert atomik.

Contoh ringkas keluaran yang diharapkan:

```text
10 concurrent workers -> 1 processed, 9 duplicate
processed_events count = 1
```

Interpretasi hasil tersebut adalah bahwa paralelisme tidak menyebabkan double-process. Walaupun worker berjalan bersamaan, database tetap menjadi penjaga tunggal integritas. Dalam kerangka sistem terdistribusi, ini penting karena concurrency tanpa kontrol akan menimbulkan anomali. Dengan hasil nol double-process, proyek menunjukkan bahwa desain dedup dan transaksi mampu mempertahankan correctness di bawah beban paralel.

## 6. Daftar Referensi (APA 7th)

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.

Python Software Foundation. (n.d.). *asyncio*. Python documentation. https://docs.python.org/3/library/asyncio.html

Redis Ltd. (n.d.). *Redis Streams*. Redis documentation. https://redis.io/docs/latest/develop/data-types/streams/

The PostgreSQL Global Development Group. (n.d.). *INSERT*. PostgreSQL documentation. https://www.postgresql.org/docs/current/sql-insert.html

FastAPI. (n.d.). *FastAPI documentation*. https://fastapi.tiangolo.com/
