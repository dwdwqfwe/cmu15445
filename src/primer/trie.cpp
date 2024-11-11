#include "primer/trie.h"

#include <cstddef>

#include <iostream>

#include <memory>

#include <optional>

#include <string_view>

#include <utility>

#include "common/exception.h"

namespace bustub {

template <class T>

auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return

  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If

  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.

  // Otherwise, return the value.

  auto node = root_;

  for (auto ch : key) {
    if (node == nullptr || node->children_.find(ch) == node->children_.end()) {
      // std::cout<<"null  "<<node->children_.size()<< "\n";

      return nullptr;
    }

    node = node->children_.at(ch);
  }

  const auto *res = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());

  if (res != nullptr) {
    return res->value_.get();
  }

  return nullptr;
}

template <class T>

auto Trie::PutDfs(const std::shared_ptr<TrieNode> &node, std::string_view key, size_t pos, T value) const -> void {
  std::shared_ptr<TrieNode> new_root(new TrieNode);

  // if(pos==0){

  //   std::cout<<value<<"  value\n";

  // }

  if (pos < key.size() - 1) {
    if (node->children_.find(key[pos]) == node->children_.end()) {
      node->children_.insert({key[pos], new_root});

      // if (node->children_.at(key[pos]) == new_root) {

      // }

    } else {
      new_root = node->children_.at(key[pos])->Clone();

      node->children_[key[pos]] = new_root;
    }

    PutDfs<T>(new_root, key, pos + 1, std::move(value));

    return;
  }

  if (pos == key.size() - 1) {
    if (node->children_.find(key[pos]) == node->children_.end()) {
      auto val_p = std::make_shared<T>(std::move(value));

      node->children_.insert({key[pos], std::make_shared<const TrieNodeWithValue<T>>(std::move(val_p))});

    } else {
      auto val_p = std::make_shared<T>(std::move(value));

      auto temp = node->children_.at(key[pos]);

      node->children_.erase(key[pos]);

      node->children_.insert(

          {key[pos], std::make_shared<const TrieNodeWithValue<T>>(temp->children_, std::move(val_p))});
    }
  }
}

template <class T>

auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already

  // exists, you should create a new `TrieNodeWithValue

  std::shared_ptr<TrieNode> node;

  std::shared_ptr<TrieNode> new_root;

  if (key.empty()) {
    if (root_->children_.empty()) {
      auto val_p = std::make_shared<T>(value);

      new_root = std::make_shared<TrieNodeWithValue<T>>(std::move(val_p));

    } else {
      auto val_p = std::make_shared<T>(value);

      new_root = std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }

    return Trie(new_root);
  }

  if (root_ == nullptr) {
    new_root = std::make_shared<TrieNode>();

  } else {
    new_root = root_->Clone();
  }

  PutDfs<T>(new_root, key, 0, std::move(value));

  return Trie(new_root);
}

auto Trie::RemoveDfs(std::string_view &key, const std::shared_ptr<TrieNode> &node, size_t pos) const -> bool {
  std::shared_ptr<TrieNode> next_node = (node->children_).at(key[pos])->Clone();

  if (pos == key.size() - 1) {
    if (next_node->children_.empty()) {
      node->children_.erase(key[pos]);

      return true;
    }

    std::shared_ptr<TrieNode> no_value(new TrieNode(next_node->children_));

    next_node = no_value;

    node->children_.at(key[pos]) = next_node->Clone();

    return false;
  }

  bool flag = RemoveDfs(key, next_node, pos + 1);

  if (!flag) {
    node->children_.at(key[pos]) = next_node->Clone();

    return false;
  }

  if (next_node->children_.empty() && !next_node->is_value_node_) {
    node->children_.erase(key[pos]);

    return true;
  }

  node->children_.at(key[pos]) = next_node->Clone();

  return false;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,

  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.

  if (key.empty()) {
    if (root_->is_value_node_) {
      if (root_->children_.empty()) {
        return Trie(nullptr);
      }

      auto res = std::make_shared<TrieNode>(root_->children_);

      return Trie(res);
    }

    return *this;
  }

  std::shared_ptr<TrieNode> node = root_->Clone();

  RemoveDfs(key, node, 0);

  if (node->children_.empty() && !node->is_value_node_) {
    node = nullptr;
  }

  return Trie(std::move(node));
}

// Below are explicit instantiation of template functions.

//

// Generally people would write the implementation of template classes and functions in the header file. However, we

// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the

// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up

// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;

template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;

template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;

template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

// using Integer = std::unique_ptr<uint32_t>;

// template auto Trie::Put(std::string_view key, Integer value) const -> Trie;

// template auto Trie::Get(std::string_view key) const -> const Integer *;

// template auto Trie::Put(std::string_view key, MoveBlocked valucde) const -> Trie;

// template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub