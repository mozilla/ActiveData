
ActiveData Query Tool
=====================

Requirements
------------

Access to one of the bug clusters is required.  Use [Elasticsearch Head](https://github.com/mobz/elasticsearch-head) to
test connectivity.


  - HTTPS proxy to public cluster<br> ```https://esfrontline.bugzilla.mozilla.org:443/public_bugs/bug_version```
  - Non-encrypted proxy to public cluster<br>```http://esfrontline.bugzilla.mozilla.org:80/public_bugs/bug_version```
  - Direct to public cluster (need VPN access)<br>```http://elasticsearch-zlb.bugs.scl3.mozilla.com:9200/public_bugs/bug_version```
  - Direct to private cluster (need VPN access)<br>```http://elasticsearch-private.bugs.scl3.mozilla.com:9200/private_bugs/bug_version```

Documentation
-------------

- [Using this Query Tool](../docs/ActiveData Query Tool.pdf)
- [Unittest Tutorial](../docs/jx_tutorial.md)
- [JSON Expressions](../docs/jx_expressions.md)
- [Reference Documentation](../docs/jx_reference.md)
- [Bugzilla Tutorial](../docs/BZ_Tutorial.md)
- [ActiveData code on Github](https://github.com/klahnakoski/ActiveData)

