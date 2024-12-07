import {
  OutputSchema as RepoEvent,
  isCommit,
} from './lexicon/types/com/atproto/sync/subscribeRepos'
import { ViewExternal } from './lexicon/types/app/bsky/embed/external'
import { FirehoseSubscriptionBase, getOpsByType } from './util/subscription'
import { URL } from "url";

interface Post {
	uri: string;
	cid: string;
	indexedAt: string;
}

export class FirehoseSubscription extends FirehoseSubscriptionBase {
  async handleEvent(evt: RepoEvent) {
    if (!isCommit(evt)) return

    const ops = await getOpsByType(evt)
    const postsToDelete = ops.posts.deletes.map((del) => del.uri);
    const postsToCreate: Post[] = []; 

		for (const post of ops.posts.creates) {
			if (post.record.embed && post.record.embed.external) {
				const embed = post.record.embed.external as ViewExternal;
				const uri = new URL(embed.uri);

				if (uri.pathname.includes('/p/')) {
					if (uri.hostname.endsWith('substack.com')) {
						postsToCreate.push({
							uri: post.uri,
							cid: post.cid,
							indexedAt: new Date().toISOString(),
						});
					} else {
						try {
							const response = await fetch(uri, {method: "HEAD", redirect: "follow"});
							const cluster = response.headers.get('x-cluster');

							if (cluster === 'substack') {
								postsToCreate.push({
									uri: post.uri,
									cid: post.cid,
									indexedAt: new Date().toISOString(),
								});
							}
						} catch {
							// ignore
						}
					}
				}
			}
		}

    if (postsToDelete.length > 0) {
      await this.db
        .deleteFrom('post')
        .where('uri', 'in', postsToDelete)
        .execute()
    }
    if (postsToCreate.length > 0) {
      await this.db
        .insertInto('post')
        .values(postsToCreate)
        .onConflict((oc) => oc.doNothing())
        .execute()
    }
  }
}
