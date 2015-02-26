
ActiveData Query Tool
=====================

Requirements
------------

Access to one of the bug clusters is required.  Use [ElasticSearch Head](https://github.com/mobz/elasticsearch-head) to
test connectivity.


  - HTTPS proxy to public cluster<br> ```https://esfrontline.bugzilla.mozilla.org:443/public_bugs/bug_version```
  - Non-encrypted proxy to public cluster<br>```http://esfrontline.bugzilla.mozilla.org:80/public_bugs/bug_version```
  - Direct to public cluster (need VPN access)<br>```http://elasticsearch-zlb.bugs.scl3.mozilla.com:9200/public_bugs/bug_version```
  - Direct to private cluster (need VPN access)<br>```http://elasticsearch-private.bugs.scl3.mozilla.com:9200/private_bugs/bug_version```

Documentation
-------------

  - [Tutorial on querying ElasticSearch](../docs/BZ_Tutorial.md)
  - [Tutorial on MVEL and advanced querying](../docs/MVEL_Tutorial.md)
  - [Reference document covering the query format](../docs/Qb_Reference.md)
  - [Dimension Definitions](../docs/Dimension Definitions.md)
