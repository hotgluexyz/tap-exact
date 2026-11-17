from hotglue_smoke_test.vcr.tap import VCRTapTestRunner


class ConnectorTapTestRunner(VCRTapTestRunner):
    # Exact answers OData Atom XML; xmltodict keys keep their namespace prefix and
    # attributes are "@"-prefixed (matched here without the "@").
    PRESERVE_KEYS = {
        # XML namespace declarations, so the scrubbed body still parses the same way
        "xmlns",
        "xmlns:d",
        "xmlns:m",
        "xml:base",
        # EDM type attribute: post_process maps Edm.Boolean values to bools
        "m:type",
        "m:null",
        # feed <link rel="next" href="...$skiptoken=..."> drives pagination
        "rel",
        "href",
        # primary keys: parent ids are embedded in child stream request URLs
        "d:ID",
        "d:OrderID",
        "d:InvoiceID",
        "d:EntryID",
        "d:Item",
        # replication keys feed the STATE bookmarks
        "d:Modified",
        "d:Timestamp",
        "d:sysmodified",
        "d:OrderDate",
    }

    def module(self) -> str:
        return "tap_exact"

    def launch(self):
        from tap_exact.tap import TapExact

        TapExact.cli()


if __name__ == "__main__":
    ConnectorTapTestRunner.main()
