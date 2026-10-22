package de.plmail.core.data

import de.plmail.core.database.PlMailDatabase
import de.plmail.jmap.client.JmapClient
import de.plmail.jmap.mail.Comparator
import de.plmail.jmap.mail.EmailFilter
import de.plmail.jmap.methods.EmailGet
import de.plmail.jmap.methods.EmailQuery
import de.plmail.jmap.methods.ThreadGet
import de.plmail.jmap.protocol.AccountId
import de.plmail.jmap.protocol.Capability
import de.plmail.jmap.protocol.MailboxId
import de.plmail.jmap.protocol.RequestBuilder
import kotlinx.coroutines.flow.first

/**
 * Brings the newest conversation of every tab the cache holds none of onto the device.
 *
 * The sidebar draws a tab while it holds mail, which is the web's rule, but the phone can only ask
 * its cache, and until now nothing put a tab's mail in the cache before somebody opened the tab.
 * Primary pages `threadInboxTab = primary`, and the other tabs' mail arrived only as *new* mail
 * through delta sync. A tab whose mail had all been filed before this device looked was never
 * drawn, and so could never be opened and paged either. That is typically one of the reader's own
 * categories, filled once by a filter. See #52.
 *
 * So the newest conversation of each such tab is fetched the way [AccountPager] fetches a page, and
 * stored the way a page is. The cache still answers whether a tab is drawn, and the rule stays
 * [CategoryDigest.populated]'s. The rows fetched here are ordinary cached conversations, which
 * delta sync keeps current afterwards: archive the conversation on the web and the tab goes, as it
 * does there.
 *
 * **Most of the time this asks nothing.** Primary is drawn whatever the cache holds, and a tab the
 * cache already has a conversation in needs no evidence from the server. On a device that has been
 * running a while that is nearly every tab. What is left goes out as one request of three
 * back-referenced calls a tab, split only where the server's `maxCallsInRequest` demands it. A tab
 * that is empty on the server too is asked again on the next refresh. One small request per
 * directory refresh is cheaper than remembering an answer that goes stale the moment a filter files
 * something.
 *
 * Nothing at all on a server without the extension, for an account the reader keeps as one list (no
 * tabs are drawn there), or before the account's inbox is known. A tab is an inbox idea, and a
 * conversation stored without its inbox to place it by would hold no tab open.
 *
 * Failures are the caller's to swallow: this is a background top-up behind a list already drawn,
 * and a tab that stays hidden one refresh longer is the behaviour from before it existed.
 */
internal suspend fun seedEmptyTabs(
    database: PlMailDatabase,
    mail: MailRepository,
    client: JmapClient,
    accountKey: String,
    accountId: AccountId,
) {
    val session = client.session()
    if (!session.supportsInboxTabs(accountId)) return
    if (!database.wantsInboxTabs(accountKey)) return

    val enabled = database.enabledTabKeys(accountKey) ?: return
    val inbox = database.mailboxes().byRole(accountKey, INBOX_ROLE) ?: return

    val held =
        database
            .threads()
            .observeTabPlacements()
            .first()
            .filter { it.accountKey == accountKey }
            .mapNotNullTo(mutableSetOf()) { tabKeyOf(it.inboxTab, it.category, enabled) }

    val empty =
        database
            .inboxTabs()
            .forAccount(accountKey)
            .filter { it.isEnabled && it.tabId != MailCategory.PRIMARY.wire }
            .map { it.tabId }
            .filter { it !in held }

    if (empty.isEmpty()) return

    val maxCalls = session.core.maxCallsInRequest

    empty.chunked((maxCalls / CALLS_PER_TAB).coerceAtLeast(1)).forEach { chunk ->
        val request =
            RequestBuilder(using = Capability.USING_INBOX_TABS, maxCallsInRequest = maxCalls)

        val calls = chunk.map { tab ->
            val query =
                request.add(
                    EmailQuery(
                        accountId = accountId,
                        filter =
                            EmailFilter.And(
                                listOf(
                                    EmailFilter.InMailbox(MailboxId(inbox.mailboxId)),
                                    EmailFilter.ThreadInboxTab(tab),
                                )
                            ),
                        sort = listOf(Comparator.NEWEST_FIRST),
                        collapseThreads = true,
                        limit = 1,
                    )
                )
            val get = request.add(EmailGet.byReference(accountId, query.reference("/ids")))
            val threads =
                request.add(ThreadGet.forEmailsOf(accountId, get.reference("/list/*/threadId")))

            Triple(query, get, threads)
        }

        val results = client.send(request)

        // Each tab on its own: a key the server has stopped recognising since the
        // tabs were read is refused with invalidArguments, and that refusal takes
        // its own three calls down and nothing else.
        calls.forEach { (query, get, threads) ->
            val ids = results.resultOrNull(query)?.ids ?: return@forEach
            val found = results.resultOrNull(get) ?: return@forEach

            mail.storeEmails(
                accountKey,
                found.ordered(ids),
                results.resultOrNull(threads)?.list.orEmpty(),
                fetchedAt = System.currentTimeMillis(),
            )

            // As a page does, and with the same "only where absent": the rows
            // just stored are delta sync's to keep current, and that needs a
            // cursor no later than the read that produced them.
            mail.recordEmailState(accountKey, found.state)
        }
    }
}

/** An `Email/query`, the `Email/get` of its ids and the `Thread/get` of their conversations. */
private const val CALLS_PER_TAB = 3

private const val INBOX_ROLE = "inbox"
