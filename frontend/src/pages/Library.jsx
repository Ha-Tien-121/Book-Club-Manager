import { useState } from "react";
import { BookMarked, BookOpen, CheckCircle2 } from "lucide-react";
import BookCard from "../components/BookCard";
import { library } from "../data/mockData";

const tabs = [
  { key: "inProgress", label: "In Progress", icon: BookOpen, count: library.inProgress.length },
  { key: "saved", label: "Saved", icon: BookMarked, count: library.saved.length },
  { key: "finished", label: "Finished", icon: CheckCircle2, count: library.finished.length },
];

export default function LibraryPage() {
  const [activeTab, setActiveTab] = useState("inProgress");

  const currentBooks = library[activeTab];

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10">
      <div className="mb-10">
        <h1 className="text-3xl sm:text-4xl font-serif font-bold text-warm-900">
          Your Library
        </h1>
        <p className="text-warm-500 mt-2">
          Track what you're reading, save books for later, and revisit old favorites.
        </p>
      </div>

      {/* Tabs */}
      <div className="flex gap-1 p-1 bg-warm-100 rounded-xl w-fit mb-8">
        {tabs.map(({ key, label, icon: Icon, count }) => (
          <button
            key={key}
            onClick={() => setActiveTab(key)}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all ${
              activeTab === key
                ? "bg-warm-50 text-warm-800 shadow-sm"
                : "text-warm-500 hover:text-warm-700"
            }`}
          >
            <Icon className="w-4 h-4" />
            {label}
            <span
              className={`ml-1 px-1.5 py-0.5 text-xs rounded-full ${
                activeTab === key
                  ? "bg-sage-100 text-sage-700"
                  : "bg-warm-200 text-warm-500"
              }`}
            >
              {count}
            </span>
          </button>
        ))}
      </div>

      {currentBooks.length === 0 ? (
        <div className="text-center py-16 text-warm-400">
          <p className="text-lg">
            {activeTab === "inProgress" && "You're not reading anything right now."}
            {activeTab === "saved" && "No saved books yet."}
            {activeTab === "finished" && "No finished books yet."}
          </p>
          <p className="text-sm mt-2">
            Browse the <a href="/" className="text-sage-600 underline">Feed</a> to find your next book.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-5">
          {currentBooks.map((book) => (
            <BookCard key={book.id} book={book} />
          ))}
        </div>
      )}
    </div>
  );
}
