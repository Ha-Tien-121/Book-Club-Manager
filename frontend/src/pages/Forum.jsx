import { useState } from "react";
import { MessageSquare, Heart, MessageCircle, Filter } from "lucide-react";
import { forumPosts, genres } from "../data/mockData";

export default function Forum() {
  const [filter, setFilter] = useState("all");

  const filtered =
    filter === "all"
      ? forumPosts
      : filter === "clubs"
        ? forumPosts.filter((p) => p.club)
        : forumPosts.filter((p) => !p.club);

  return (
    <div className="max-w-3xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <div className="mb-10">
        <h1 className="text-3xl sm:text-4xl font-serif font-bold text-warm-900">
          Forum
        </h1>
        <p className="text-warm-500 mt-2">
          Discuss books, share recommendations, and connect with fellow readers.
        </p>
      </div>

      {/* Filter tabs */}
      <div className="flex gap-1 p-1 bg-warm-100 rounded-xl w-fit mb-8">
        {[
          { key: "all", label: "All Posts" },
          { key: "public", label: "Public" },
          { key: "clubs", label: "Club Discussions" },
        ].map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setFilter(key)}
            className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              filter === key
                ? "bg-warm-50 text-warm-800 shadow-sm"
                : "text-warm-500 hover:text-warm-700"
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Posts */}
      <div className="space-y-4">
        {filtered.map((post) => (
          <article
            key={post.id}
            className="p-5 bg-warm-50 rounded-2xl border border-warm-200 hover:border-warm-300 hover:shadow-sm transition-all cursor-pointer"
          >
            <div className="flex items-start gap-4">
              {/* Avatar */}
              <div className="w-10 h-10 rounded-full bg-sage-100 text-sage-600 flex items-center justify-center text-sm font-semibold shrink-0">
                {post.avatar}
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap">
                  <span className="text-sm font-medium text-warm-700">
                    {post.author}
                  </span>
                  <span className="text-xs text-warm-400">{post.timeAgo}</span>
                  {post.club && (
                    <span className="px-2 py-0.5 text-xs rounded-full bg-sage-50 text-sage-600 border border-sage-200">
                      {post.club}
                    </span>
                  )}
                  {post.genre && !post.club && (
                    <span className="px-2 py-0.5 text-xs rounded-full bg-warm-100 text-warm-500 border border-warm-200">
                      {post.genre}
                    </span>
                  )}
                </div>

                <h3 className="font-serif text-lg font-semibold text-warm-900 mt-1.5">
                  {post.title}
                </h3>
                <p className="text-sm text-warm-500 mt-1.5 line-clamp-2">
                  {post.preview}
                </p>

                <div className="flex items-center gap-5 mt-4 text-sm text-warm-400">
                  <span className="flex items-center gap-1.5 hover:text-rose-400 transition-colors">
                    <Heart className="w-4 h-4" />
                    {post.likes}
                  </span>
                  <span className="flex items-center gap-1.5 hover:text-sage-500 transition-colors">
                    <MessageCircle className="w-4 h-4" />
                    {post.replies} replies
                  </span>
                </div>
              </div>
            </div>
          </article>
        ))}
      </div>

      {/* New Post CTA */}
      <div className="mt-8 p-5 bg-sage-50 rounded-2xl border border-sage-200 text-center">
        <MessageSquare className="w-8 h-8 text-sage-400 mx-auto mb-3" />
        <p className="text-sm text-sage-600 font-medium">
          Want to start a discussion?
        </p>
        <p className="text-xs text-sage-500 mt-1">
          Forum posting will be available once you sign in.
        </p>
      </div>
    </div>
  );
}
